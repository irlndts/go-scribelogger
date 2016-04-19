package scribe

import (
	"errors"
	"fmt"
	"github.com/artyom/scribe"
	"github.com/artyom/thrift"
	"time"
)

type Scribe struct {
	transport *thrift.TFramedTransport
	socket    *thrift.TSocket
	protocol  *thrift.TBinaryProtocol
	scribe    *scribe.ScribeClient
	logChan   chan *scribe.LogEntry
	config    *ScribeConfig
}

type ScribeConfig struct {
	Address             string // Адрес сервера Скрайба,                                          / например localhost:1463
	ReconnectTimeoutSec uint16 // Таймаут между попытками при неудачном подключении (в секундах!) / например 20
	MaxPacketSize       int    // Максимальный размер отправляемого пакета (в кол-ве сообщений).  / например 50
	EntryBufferSize     uint16 // Размер очереди сообщений (в кол-ве сообщений),                  / например 1000
	// если очередь переполняется, часть логов теряется
}

//############ Функции для внешнего использования //############
// Создание нового логгера
func NewScribe(config *ScribeConfig) (s *Scribe) {
	s = &Scribe{
		config: config,
	}
	s.logChan = make(chan *scribe.LogEntry, s.config.EntryBufferSize)

	// Горутина, которая пытается подключиться к Скрайбу до конца времен
	go func() {
		for {
			err := s.connect()
			if err == nil {
				fmt.Printf("Scribe %s connect ok\n", s.config.Address)
				//connectSpy.End()
				return
			} else {
				timeout := time.Duration(s.config.ReconnectTimeoutSec) * time.Second
				fmt.Printf("Error connecting scribe, will retry in %v: %v\n", timeout, err)
				time.Sleep(timeout)
			}
		}
	}()

	go s.logSender() // Запускаем демон отправки логов

	return s
}

func (s *Scribe) IsOpen() bool {
	return s.scribe != nil
}

// Непосредственная отправка сообщений
func (s *Scribe) Log(cat string, msg string) {
	logEntry := &scribe.LogEntry{
		Category: cat,
		Message:  msg,
	}

	// Если канал еще не создан - выходим
	if s.logChan == nil {
		return
	}

	// Пытаемся отправить сообщение в канал
	// Если буфер канала полный, теряем сообщение
	select {
	case s.logChan <- logEntry:
	default:
	}
}

//############ Внутренние функции модуля //############

// Горутина для постоянной отправки пачек логов
func (s *Scribe) logSender() {
	for {
		s.sendLogBuffer()
	}
}

// Отправка порции логов на сервер
func (s *Scribe) sendLogBuffer() {
	var buffer []*scribe.LogEntry
	var entry *scribe.LogEntry

	// Если еще не подключились к серверу, то попробуем снова отправить через 100 мс
	if s.scribe == nil {
		time.Sleep(100 * time.Millisecond)
		return
	}

	// Ждем хотя-бы одну запись в канале
	entry = <-s.logChan
	buffer = append(buffer, entry)

	cont := true // Переменная для выхода из цикла, ибо break внутри select не работает
	for cont {
		// Если насобирали на максимальный размер пакета - отправляем, не раздумывая
		if len(buffer) >= s.config.MaxPacketSize {
			break
		}

		// Собираем все записи, накопившиеся в канале
		select {
		case entry = <-s.logChan:
			// и пихаем их в массив buffer
			buffer = append(buffer, entry)
		default:
			cont = false
		}
	}

	// Если вообще что-то собрали
	if len(buffer) > 0 {
		// Отправляем пачку собранных записей на сервер
		s.scribe.Log(buffer)
		// result, err := s.scribe.Log(buffer)
		// log.Printf("Sent %d entries. Result: %v. Error: %v\n", len(buffer), result, err)
	}
}

func (s *Scribe) connect() error {
	var err error
	s.socket, err = thrift.NewTSocket(s.config.Address)
	if err != nil {
		return err
	}
	s.transport = thrift.NewTFramedTransport(s.socket)
	s.protocol = thrift.NewTBinaryProtocolTransport(s.transport)
	s.transport.Open()
	if !s.transport.IsOpen() {
		return errors.New("Cannot open transport")
	}
	s.scribe = scribe.NewScribeClientProtocol(s.transport, s.protocol, s.protocol)
	if err != nil {
		return err
	}
	return nil
}
