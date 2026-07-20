//go:build complemau

package complemau_tests

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/matrix-org/complement/b"
	"github.com/matrix-org/complement/client"

	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/appservice"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type complemauQueue[T any] struct {
	mu    sync.Mutex
	items []T
	ready chan struct{}
}

func newComplemauQueue[T any]() *complemauQueue[T] {
	return &complemauQueue[T]{ready: make(chan struct{}, 1)}
}

func (q *complemauQueue[T]) push(item T) {
	q.mu.Lock()
	q.items = append(q.items, item)
	q.mu.Unlock()
	select {
	case q.ready <- struct{}{}:
	default:
	}
}

func (q *complemauQueue[T]) pop(ctx context.Context) (T, bool) {
	var zero T
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			item := q.items[0]
			q.items[0] = zero
			q.items = q.items[1:]
			more := len(q.items) > 0
			q.mu.Unlock()
			if more {
				select {
				case q.ready <- struct{}{}:
				default:
				}
			}
			return item, true
		}
		q.mu.Unlock()

		select {
		case <-q.ready:
		case <-ctx.Done():
			return zero, false
		}
	}
}

func mustPopComplemau[T any](t *testing.T, queue *complemauQueue[T], timeout time.Duration, description string) T {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	item, ok := queue.pop(ctx)
	if !ok {
		t.Fatalf("complemau: timed out after %s waiting for %s", timeout, description)
	}
	return item
}

type complemauQueryHandler struct {
	users   *complemauQueue[id.UserID]
	aliases *complemauQueue[id.RoomAlias]

	mu         sync.RWMutex
	queryUser  func(id.UserID) bool
	queryAlias func(id.RoomAlias) bool
}

func newComplemauQueryHandler() *complemauQueryHandler {
	return &complemauQueryHandler{
		users:      newComplemauQueue[id.UserID](),
		aliases:    newComplemauQueue[id.RoomAlias](),
		queryUser:  func(id.UserID) bool { return false },
		queryAlias: func(id.RoomAlias) bool { return false },
	}
}

func (handler *complemauQueryHandler) QueryUser(userID id.UserID) bool {
	handler.users.push(userID)
	handler.mu.RLock()
	defer handler.mu.RUnlock()
	return handler.queryUser(userID)
}

func (handler *complemauQueryHandler) QueryAlias(alias id.RoomAlias) bool {
	handler.aliases.push(alias)
	handler.mu.RLock()
	defer handler.mu.RUnlock()
	return handler.queryAlias(alias)
}

func (handler *complemauQueryHandler) setUserResult(query func(id.UserID) bool) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	handler.queryUser = query
}

func (handler *complemauQueryHandler) setAliasResult(query func(id.RoomAlias) bool) {
	handler.mu.Lock()
	defer handler.mu.Unlock()
	handler.queryAlias = query
}

type complemauTransaction struct {
	ID         string
	ReceivedAt time.Time
	Body       *appservice.Transaction
}

type complemauOTKTransaction struct {
	Counts       appservice.OTKCountMap
	FallbackKeys appservice.FallbackKeyMap
}

type complemauTxnResponse struct {
	status    int
	release   <-chan struct{}
	responded chan struct{}
}

type complemauEndpoint string

const (
	complemauThirdPartyProtocol complemauEndpoint = "third_party_protocol"
	complemauThirdPartyUser     complemauEndpoint = "third_party_user"
	complemauThirdPartyLocation complemauEndpoint = "third_party_location"
	complemauKeyClaim           complemauEndpoint = "key_claim"
	complemauKeyQuery           complemauEndpoint = "key_query"
)

type complemauEndpointRequest struct {
	Method   string
	Path     string
	Protocol string
	Query    url.Values
	Header   http.Header
	Body     json.RawMessage
}

type complemauEndpointResponse struct {
	Status int
	Body   any
}

type complemauRoute struct {
	requests  *complemauQueue[*complemauEndpointRequest]
	responses chan complemauEndpointResponse
}

func newComplemauRoute() *complemauRoute {
	return &complemauRoute{
		requests:  newComplemauQueue[*complemauEndpointRequest](),
		responses: make(chan complemauEndpointResponse, 16),
	}
}

// complemauBridge is a mautrix appservice standing in for a real bridge. Its
// EventProcessor continuously drains every receive channel into lossless test
// queues, so transaction handling never waits for a test assertion.
type complemauBridge struct {
	as        *appservice.AppService
	processor *appservice.EventProcessor
	ctx       context.Context
	cancel    context.CancelFunc
	stopOnce  sync.Once
	port      uint16

	pdus         *complemauQueue[*event.Event]
	receipts     *complemauQueue[*event.Event]
	typing       *complemauQueue[*event.Event]
	presence     *complemauQueue[*event.Event]
	toDevice     *complemauQueue[*event.Event]
	deviceLists  *complemauQueue[*mautrix.DeviceLists]
	otkCounts    *complemauQueue[*mautrix.OTKCount]
	otkTxns      *complemauQueue[*complemauOTKTransaction]
	transactions *complemauQueue[*complemauTransaction]
	pings        *complemauQueue[string]
	queries      *complemauQueryHandler
	routes       map[complemauEndpoint]*complemauRoute
	responses    chan complemauTxnResponse
	seenMu       sync.Mutex
	seenTxnIDs   map[string]struct{}
}

func startComplemauBridge(t *testing.T, homeserverURL string) *complemauBridge {
	t.Helper()
	registration := b.BlueprintHSWithComplemauBridge.Homeservers[0].ApplicationServices[0]
	return startComplemauBridgeWithRegistration(t, homeserverURL, registration, b.ComplemauASPort)
}

func startComplemauBridges(t *testing.T, homeserverURL string) (*complemauBridge, *complemauBridge) {
	t.Helper()
	registrations := b.BlueprintHSWithTwoComplemauBridges.Homeservers[0].ApplicationServices
	first := startComplemauBridgeWithRegistration(t, homeserverURL, registrations[0], b.ComplemauASPort)
	second := startComplemauBridgeWithRegistration(t, homeserverURL, registrations[1], b.ComplemauSecondASPort)
	return first, second
}

func startComplemauBridgeWithRegistration(
	t *testing.T,
	homeserverURL string,
	blueprintReg b.ApplicationService,
	port uint16,
) *complemauBridge {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	registration := mautrixRegistration(blueprintReg)
	as, err := appservice.CreateFull(appservice.CreateOpts{
		Registration:     registration,
		HomeserverDomain: "hs1",
		HomeserverURL:    homeserverURL,
		HostConfig: appservice.HostConfig{
			Hostname: "0.0.0.0",
			Port:     port,
		},
	})
	if err != nil {
		cancel()
		t.Fatalf("complemau: failed to create mautrix appservice: %v", err)
	}

	bridge := &complemauBridge{
		as:           as,
		ctx:          ctx,
		cancel:       cancel,
		port:         port,
		pdus:         newComplemauQueue[*event.Event](),
		receipts:     newComplemauQueue[*event.Event](),
		typing:       newComplemauQueue[*event.Event](),
		presence:     newComplemauQueue[*event.Event](),
		toDevice:     newComplemauQueue[*event.Event](),
		deviceLists:  newComplemauQueue[*mautrix.DeviceLists](),
		otkCounts:    newComplemauQueue[*mautrix.OTKCount](),
		otkTxns:      newComplemauQueue[*complemauOTKTransaction](),
		transactions: newComplemauQueue[*complemauTransaction](),
		pings:        newComplemauQueue[string](),
		queries:      newComplemauQueryHandler(),
		routes: map[complemauEndpoint]*complemauRoute{
			complemauThirdPartyProtocol: newComplemauRoute(),
			complemauThirdPartyUser:     newComplemauRoute(),
			complemauThirdPartyLocation: newComplemauRoute(),
			complemauKeyClaim:           newComplemauRoute(),
			complemauKeyQuery:           newComplemauRoute(),
		},
		responses:  make(chan complemauTxnResponse, 16),
		seenTxnIDs: make(map[string]struct{}),
	}

	as.QueryHandler = bridge.queries
	bridge.installRoutes()
	bridge.startEventProcessor()

	go as.Start()
	t.Cleanup(bridge.stop)
	waitForReceiver(t, port)
	bridge.ensureReady(t)
	return bridge
}

func mautrixRegistration(blueprintReg b.ApplicationService) *appservice.Registration {
	rateLimited := blueprintReg.RateLimited
	registration := &appservice.Registration{
		ID:                  blueprintReg.ID,
		AppToken:            blueprintReg.ASToken,
		ServerToken:         blueprintReg.HSToken,
		SenderLocalpart:     blueprintReg.SenderLocalpart,
		URL:                 blueprintReg.URL,
		RateLimited:         &rateLimited,
		Protocols:           append([]string(nil), blueprintReg.Protocols...),
		EphemeralEvents:     blueprintReg.SendEphemeral,
		SoruEphemeralEvents: blueprintReg.SendEphemeral,
		MSC3202:             blueprintReg.EnableEncryption,
		MSC4190:             blueprintReg.EnableMSC4190,
	}

	namespaces := blueprintReg.Namespaces
	if namespaces == nil {
		registration.Namespaces.UserIDs = appservice.NamespaceList{{Regex: ".*"}}
		return registration
	}
	for _, namespace := range namespaces.Users {
		registration.Namespaces.UserIDs = append(registration.Namespaces.UserIDs, appservice.Namespace{
			Regex: namespace.Regex, Exclusive: namespace.Exclusive,
		})
	}
	for _, namespace := range namespaces.Aliases {
		registration.Namespaces.RoomAliases = append(registration.Namespaces.RoomAliases, appservice.Namespace{
			Regex: namespace.Regex, Exclusive: namespace.Exclusive,
		})
	}
	for _, namespace := range namespaces.Rooms {
		registration.Namespaces.RoomIDs = append(registration.Namespaces.RoomIDs, appservice.Namespace{
			Regex: namespace.Regex, Exclusive: namespace.Exclusive,
		})
	}
	return registration
}

func (bridge *complemauBridge) startEventProcessor() {
	processor := appservice.NewEventProcessor(bridge.as)
	processor.ExecMode = appservice.Sync
	processor.ExecSyncWarnTime = 0
	processor.ExecSyncTimeout = 0

	processor.On(event.EphemeralEventReceipt, func(_ context.Context, evt *event.Event) {
		bridge.receipts.push(evt)
	})
	processor.On(event.EphemeralEventTyping, func(_ context.Context, evt *event.Event) {
		bridge.typing.push(evt)
	})
	processor.On(event.EphemeralEventPresence, func(_ context.Context, evt *event.Event) {
		bridge.presence.push(evt)
	})
	processor.Start(bridge.ctx)
	bridge.processor = processor
}

func (bridge *complemauBridge) installRoutes() {
	stockRouter := bridge.as.Router
	router := http.NewServeMux()
	router.HandleFunc("PUT /_matrix/app/v1/transactions/{txnID}", func(w http.ResponseWriter, r *http.Request) {
		bridge.handleTransaction(stockRouter, w, r)
	})
	router.HandleFunc("POST /_matrix/app/v1/ping", func(w http.ResponseWriter, r *http.Request) {
		bridge.handlePing(stockRouter, w, r)
	})
	router.HandleFunc("GET /_matrix/app/v1/thirdparty/protocol/{protocol}", bridge.endpointHandler(complemauThirdPartyProtocol))
	router.HandleFunc("GET /_matrix/app/v1/thirdparty/user/{protocol}", bridge.endpointHandler(complemauThirdPartyUser))
	router.HandleFunc("GET /_matrix/app/v1/thirdparty/location/{protocol}", bridge.endpointHandler(complemauThirdPartyLocation))
	router.HandleFunc("POST /_matrix/app/unstable/org.matrix.msc3983/keys/claim", bridge.endpointHandler(complemauKeyClaim))
	router.HandleFunc("POST /_matrix/app/unstable/org.matrix.msc3984/keys/query", bridge.endpointHandler(complemauKeyQuery))
	router.Handle("/", stockRouter)
	bridge.as.Router = router
}

func (bridge *complemauBridge) handleTransaction(stockRouter http.Handler, w http.ResponseWriter, r *http.Request) {
	var response complemauTxnResponse
	select {
	case response = <-bridge.responses:
	default:
	}
	if response.responded != nil {
		defer close(response.responded)
	}

	receivedAt := time.Now()
	transactionID := r.PathValue("txnID")
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read transaction", http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))

	var transaction appservice.Transaction
	if transactionID != "" && r.Header.Get("Authorization") == "Bearer "+bridge.as.Registration.ServerToken &&
		json.Unmarshal(body, &transaction) == nil {
		bridge.recordTransaction(transactionID, receivedAt, &transaction)
	}

	recorder := httptest.NewRecorder()
	stockRouter.ServeHTTP(recorder, r)
	if response.release != nil {
		select {
		case <-response.release:
		case <-bridge.ctx.Done():
			return
		}
	}
	if response.status >= http.StatusBadRequest {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.status)
		_, _ = io.WriteString(w, `{"errcode":"M_UNKNOWN","error":"complemau injected transaction failure"}`)
		return
	}
	writeRecordedResponse(w, recorder)
}

func (bridge *complemauBridge) recordTransaction(
	transactionID string,
	receivedAt time.Time,
	transaction *appservice.Transaction,
) {
	bridge.transactions.push(&complemauTransaction{
		ID: transactionID, ReceivedAt: receivedAt, Body: transaction,
	})
	if !bridge.markTransactionSeen(transactionID) {
		return
	}

	for _, evt := range transaction.Events {
		if evt.StateKey == nil {
			evt.Type.Class = event.MessageEventType
		} else {
			evt.Type.Class = event.StateEventType
		}
		_ = evt.Content.ParseRaw(evt.Type)
		bridge.pdus.push(evt)
	}
	toDeviceEvents := transaction.ToDeviceEvents
	if toDeviceEvents == nil {
		toDeviceEvents = transaction.MSC2409ToDeviceEvents
	}
	for _, evt := range toDeviceEvents {
		evt.Type.Class = event.ToDeviceEventType
		_ = evt.Content.ParseRaw(evt.Type)
		bridge.toDevice.push(evt)
	}

	deviceLists := transaction.DeviceLists
	if deviceLists == nil {
		deviceLists = transaction.MSC3202DeviceLists
	}
	if deviceLists != nil {
		bridge.deviceLists.push(deviceLists)
	}
	counts := transaction.DeviceOTKCount
	if counts == nil {
		counts = transaction.MSC3202DeviceOTKCount
	}
	fallbackKeys := transaction.FallbackKeys
	if fallbackKeys == nil {
		fallbackKeys = transaction.MSC3202FallbackKeys
	}
	if counts != nil || fallbackKeys != nil {
		bridge.otkTxns.push(&complemauOTKTransaction{Counts: counts, FallbackKeys: fallbackKeys})
	}
	bridge.recordOTKCounts(counts)
}

func (bridge *complemauBridge) markTransactionSeen(transactionID string) bool {
	bridge.seenMu.Lock()
	defer bridge.seenMu.Unlock()
	if _, seen := bridge.seenTxnIDs[transactionID]; seen {
		return false
	}
	bridge.seenTxnIDs[transactionID] = struct{}{}
	return true
}

func (bridge *complemauBridge) recordOTKCounts(counts appservice.OTKCountMap) {
	users := make([]id.UserID, 0, len(counts))
	for userID := range counts {
		users = append(users, userID)
	}
	sort.Slice(users, func(i, j int) bool { return users[i] < users[j] })
	for _, userID := range users {
		devices := counts[userID]
		deviceIDs := make([]id.DeviceID, 0, len(devices))
		for deviceID := range devices {
			deviceIDs = append(deviceIDs, deviceID)
		}
		sort.Slice(deviceIDs, func(i, j int) bool { return deviceIDs[i] < deviceIDs[j] })
		for _, deviceID := range deviceIDs {
			count := devices[deviceID]
			count.UserID = userID
			count.DeviceID = deviceID
			bridge.otkCounts.push(&count)
		}
	}
}

func (bridge *complemauBridge) handlePing(stockRouter http.Handler, w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read ping", http.StatusBadRequest)
		return
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	var ping mautrix.ReqAppservicePing
	if json.Unmarshal(body, &ping) == nil {
		bridge.pings.push(ping.TxnID)
	}
	recorder := httptest.NewRecorder()
	stockRouter.ServeHTTP(recorder, r)
	writeRecordedResponse(w, recorder)
}

func writeRecordedResponse(w http.ResponseWriter, recorder *httptest.ResponseRecorder) {
	for name, values := range recorder.Header() {
		for _, value := range values {
			w.Header().Add(name, value)
		}
	}
	w.WriteHeader(recorder.Code)
	_, _ = io.Copy(w, recorder.Body)
}

func (bridge *complemauBridge) endpointHandler(endpoint complemauEndpoint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !bridge.as.CheckServerToken(w, r) {
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read request", http.StatusBadRequest)
			return
		}
		route := bridge.routes[endpoint]
		route.requests.push(&complemauEndpointRequest{
			Method: r.Method, Path: r.URL.Path, Protocol: r.PathValue("protocol"),
			Query: r.URL.Query(), Header: r.Header.Clone(), Body: append(json.RawMessage(nil), body...),
		})

		response := complemauEndpointResponse{Status: http.StatusOK, Body: map[string]any{}}
		select {
		case response = <-route.responses:
		default:
		}
		if response.Status == 0 {
			response.Status = http.StatusOK
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.Status)
		if response.Body == nil {
			response.Body = map[string]any{}
		}
		_ = json.NewEncoder(w).Encode(response.Body)
	}
}

func (bridge *complemauBridge) ensureReady(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(bridge.ctx, 40*time.Second)
	defer cancel()
	if !bridge.as.BotIntent().EnsureAppserviceConnection(ctx) {
		t.Fatal("complemau: homeserver could not complete the appservice readiness ping")
	}
}

func (bridge *complemauBridge) stop() {
	bridge.stopOnce.Do(func() {
		bridge.cancel()
		if bridge.processor != nil {
			bridge.processor.Stop()
		}
		bridge.as.Stop()
	})
}

func (bridge *complemauBridge) mustReceivePDU(t *testing.T, timeout time.Duration) *event.Event {
	t.Helper()
	return mustPopComplemau(t, bridge.pdus, timeout, "a PDU transaction")
}

func (bridge *complemauBridge) mustReceiveReceipt(t *testing.T, timeout time.Duration) *event.Event {
	t.Helper()
	return mustPopComplemau(t, bridge.receipts, timeout, "an m.receipt transaction")
}

func (bridge *complemauBridge) mustReceiveTyping(t *testing.T, timeout time.Duration) *event.Event {
	t.Helper()
	return mustPopComplemau(t, bridge.typing, timeout, "an m.typing transaction")
}

func (bridge *complemauBridge) mustReceivePresence(t *testing.T, timeout time.Duration) *event.Event {
	t.Helper()
	return mustPopComplemau(t, bridge.presence, timeout, "an m.presence transaction")
}

func (bridge *complemauBridge) mustReceiveToDevice(t *testing.T, timeout time.Duration) *event.Event {
	t.Helper()
	return mustPopComplemau(t, bridge.toDevice, timeout, "a to-device transaction")
}

func (bridge *complemauBridge) mustReceiveDeviceList(t *testing.T, timeout time.Duration) *mautrix.DeviceLists {
	t.Helper()
	return mustPopComplemau(t, bridge.deviceLists, timeout, "a device-list transaction")
}

func (bridge *complemauBridge) mustReceiveOTKCount(t *testing.T, timeout time.Duration) *mautrix.OTKCount {
	t.Helper()
	return mustPopComplemau(t, bridge.otkCounts, timeout, "an OTK count")
}

func (bridge *complemauBridge) mustReceiveOTKTransaction(t *testing.T, timeout time.Duration) *complemauOTKTransaction {
	t.Helper()
	return mustPopComplemau(t, bridge.otkTxns, timeout, "OTK and fallback-key transaction data")
}

func (bridge *complemauBridge) mustReceiveTransaction(t *testing.T, timeout time.Duration) *complemauTransaction {
	t.Helper()
	return mustPopComplemau(t, bridge.transactions, timeout, "an appservice transaction")
}

func (bridge *complemauBridge) mustNotReceiveTransaction(t *testing.T, window time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	if transaction, ok := bridge.transactions.pop(ctx); ok {
		t.Errorf("complemau: unexpectedly received transaction %s", transaction.ID)
	}
}

func (bridge *complemauBridge) mustReceivePing(t *testing.T, timeout time.Duration) string {
	t.Helper()
	return mustPopComplemau(t, bridge.pings, timeout, "an appservice ping")
}

func (bridge *complemauBridge) mustNotReceiveReceipt(t *testing.T, window time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), window)
	defer cancel()
	if received, ok := bridge.receipts.pop(ctx); ok {
		t.Errorf("complemau: homeserver re-emitted an m.receipt for a non-advancing read position (tuwunel#516): %v", received.Content.Raw)
	}
}

func (bridge *complemauBridge) failNextTransaction(t *testing.T, status int) <-chan struct{} {
	t.Helper()
	if status < http.StatusBadRequest {
		t.Fatalf("complemau: injected transaction status must be an error, got %d", status)
	}
	responded := make(chan struct{})
	select {
	case bridge.responses <- complemauTxnResponse{status: status, responded: responded}:
	default:
		t.Fatal("complemau: transaction response queue is full")
	}
	return responded
}

func (bridge *complemauBridge) mustAwaitTransactionResponse(t *testing.T, responded <-chan struct{}) {
	t.Helper()
	select {
	case <-responded:
	case <-bridge.ctx.Done():
		t.Fatal("complemau: receiver stopped before writing the transaction response")
	case <-time.After(20 * time.Second):
		t.Fatal("complemau: timed out waiting for the transaction response")
	}
}

func (bridge *complemauBridge) stallNextTransaction(t *testing.T) func() {
	t.Helper()
	release := make(chan struct{})
	var once sync.Once
	releaseResponse := func() { once.Do(func() { close(release) }) }
	t.Cleanup(releaseResponse)
	select {
	case bridge.responses <- complemauTxnResponse{release: release}:
	default:
		t.Fatal("complemau: transaction response queue is full")
	}
	return releaseResponse
}

func (bridge *complemauBridge) setUserQueryResult(query func(id.UserID) bool) {
	bridge.queries.setUserResult(query)
}

func (bridge *complemauBridge) setAliasQueryResult(query func(id.RoomAlias) bool) {
	bridge.queries.setAliasResult(query)
}

func (bridge *complemauBridge) mustReceiveUserQuery(t *testing.T, timeout time.Duration) id.UserID {
	t.Helper()
	return mustPopComplemau(t, bridge.queries.users, timeout, "an appservice user query")
}

func (bridge *complemauBridge) mustReceiveAliasQuery(t *testing.T, timeout time.Duration) id.RoomAlias {
	t.Helper()
	return mustPopComplemau(t, bridge.queries.aliases, timeout, "an appservice alias query")
}

func (bridge *complemauBridge) queueEndpointResponse(
	t *testing.T,
	endpoint complemauEndpoint,
	status int,
	body any,
) {
	t.Helper()
	route, ok := bridge.routes[endpoint]
	if !ok {
		t.Fatalf("complemau: unknown appservice endpoint %q", endpoint)
	}
	select {
	case route.responses <- complemauEndpointResponse{Status: status, Body: body}:
	default:
		t.Fatalf("complemau: response queue for %q is full", endpoint)
	}
}

func (bridge *complemauBridge) mustReceiveEndpointRequest(
	t *testing.T,
	endpoint complemauEndpoint,
	timeout time.Duration,
) *complemauEndpointRequest {
	t.Helper()
	route, ok := bridge.routes[endpoint]
	if !ok {
		t.Fatalf("complemau: unknown appservice endpoint %q", endpoint)
	}
	return mustPopComplemau(t, route.requests, timeout, "an appservice endpoint request")
}

func (bridge *complemauBridge) mustCreateGhostDevice(
	t *testing.T,
	userID string,
	deviceID string,
	displayName string,
) *mautrix.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(bridge.ctx, 20*time.Second)
	defer cancel()
	intent := bridge.as.Intent(id.UserID(userID))
	if err := intent.EnsureRegistered(ctx); err != nil {
		t.Fatalf("complemau: failed to register ghost %s: %v", userID, err)
	}
	if err := intent.CreateDeviceMSC4190(ctx, id.DeviceID(deviceID), displayName); err == nil {
		return intent.Client
	} else {
		fallbackClient := bridge.as.NewMautrixClient(id.UserID(userID))
		_, loginErr := fallbackClient.Login(ctx, &mautrix.ReqLogin{
			Type:                     mautrix.AuthTypeAppservice,
			Identifier:               mautrix.UserIdentifier{Type: mautrix.IdentifierTypeUser, User: userID},
			DeviceID:                 id.DeviceID(deviceID),
			InitialDeviceDisplayName: displayName,
			StoreCredentials:         true,
		})
		if loginErr != nil {
			t.Fatalf("complemau: MSC4190 device creation failed for %s: %v; appservice login fallback failed: %v", userID, err, loginErr)
		}
		return fallbackClient
	}
}

// waitForReceiver blocks until the mautrix HTTP server answers its liveness
// probe, so the homeserver's first transaction push is not lost.
func waitForReceiver(t *testing.T, port uint16) {
	t.Helper()
	probe := "http://127.0.0.1:" + strconv.Itoa(int(port)) + "/_matrix/mau/live"
	probeClient := http.Client{Timeout: 250 * time.Millisecond}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		response, err := probeClient.Get(probe)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("complemau: mautrix receiver did not become ready on port %d", port)
}

// registerAppserviceGhost provisions a namespaced user through appservice
// registration, the way a bridge creates a ghost before puppeting it.
func registerAppserviceGhost(t *testing.T, bridge *client.CSAPI, localpart string) {
	t.Helper()
	bridge.MustDo(t, "POST", []string{"_matrix", "client", "v3", "register"},
		client.WithJSONBody(t, map[string]interface{}{
			"type":          "m.login.application_service",
			"username":      localpart,
			"inhibit_login": true,
		}),
	)
}

// asUser masquerades a bridge request as userID.
func asUser(userID string) client.RequestOpt {
	return client.WithQueries(url.Values{"user_id": {userID}})
}

func asUserDevice(userID, deviceID string) client.RequestOpt {
	return client.WithQueries(url.Values{"user_id": {userID}, "device_id": {deviceID}})
}

// postReadReceipt sends an m.read receipt for eventID.
func postReadReceipt(t *testing.T, c *client.CSAPI, roomID, eventID string, opts ...client.RequestOpt) {
	t.Helper()
	opts = append([]client.RequestOpt{client.WithJSONBody(t, struct{}{})}, opts...)
	c.MustDo(t, "POST",
		[]string{"_matrix", "client", "v3", "rooms", roomID, "receipt", "m.read", eventID},
		opts...,
	)
}
