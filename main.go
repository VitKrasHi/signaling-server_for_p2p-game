package main

import (
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/gorilla/websocket"
)

// upgrader настраивает WebSocket-соединение.
// CheckOrigin возвращает true для простоты — в продакшене стоит ограничить.
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// SignalMessage — формат сообщений между игрой и сервером.
type SignalMessage struct {
	Type       string `json:"type"`                  // "create" | "join" | "created" | "peer_addr" | "error"
	SessionID  string `json:"session_id,omitempty"`  // ID сессии
	PublicAddr string `json:"public_addr,omitempty"` // публичный IP:PORT игрока
	Text       string `json:"text,omitempty"`        // текст ошибки
}

// Peer — участник сессии (хост или гость).
type Peer struct {
	PublicAddr string
	Conn       *websocket.Conn
}

// Session — сессия, созданная хостом. Хранит обоих участников.
type Session struct {
	Host  *Peer
	Guest *Peer
}

var (
	sessions = make(map[string]*Session)
	mu       sync.Mutex
)

func handleWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("upgrade error:", err)
		return
	}
	defer conn.Close()

	var mySessionID string
	var amHost bool

	for {
		var msg SignalMessage
		if err := conn.ReadJSON(&msg); err != nil {
			// Соединение закрыто — выходим из цикла
			break
		}

		switch msg.Type {
		case "create":
			// Хост создаёт сессию
			amHost = true
			mySessionID = msg.SessionID

			mu.Lock()
			sessions[mySessionID] = &Session{
				Host: &Peer{PublicAddr: msg.PublicAddr, Conn: conn},
			}
			mu.Unlock()

			conn.WriteJSON(SignalMessage{Type: "created", SessionID: mySessionID})
			log.Printf("session %s created by %s", mySessionID, msg.PublicAddr)

		case "join":
			// Клиент присоединяется к сессии
			mySessionID = msg.SessionID

			mu.Lock()
			sess, ok := sessions[mySessionID]
			var hostAddr string
			if ok && sess.Host != nil {
				sess.Guest = &Peer{PublicAddr: msg.PublicAddr, Conn: conn}
				hostAddr = sess.Host.PublicAddr
			}
			mu.Unlock()

			if hostAddr == "" {
				conn.WriteJSON(SignalMessage{Type: "error", Text: "session not found"})
				continue
			}

			// Клиенту — адрес хоста
			conn.WriteJSON(SignalMessage{Type: "peer_addr", PublicAddr: hostAddr})
			// Хосту — адрес клиента
			sess.Host.Conn.WriteJSON(SignalMessage{Type: "peer_addr", PublicAddr: msg.PublicAddr})
			log.Printf("guest %s joined session %s", msg.PublicAddr, mySessionID)
		}
	}

	// Уборка при отключении
	mu.Lock()
	if sess, ok := sessions[mySessionID]; ok {
		if amHost {
			delete(sessions, mySessionID)
			log.Printf("session %s removed (host disconnected)", mySessionID)
		} else {
			sess.Guest = nil
			log.Printf("guest left session %s", mySessionID)
		}
	}
	mu.Unlock()
}

func main() {
	http.HandleFunc("/ws", handleWS)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("p2p signaling server is running"))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf("listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}
