package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type ExchangeSpec struct {
	Name string
	Kind string
}

type RabbitPublisher struct {
	ch      *amqp091.Channel
	returns <-chan amqp091.Return
	mu      sync.Mutex
}

func NewRabbitPublisher(conn *amqp091.Connection, exchanges []ExchangeSpec) (*RabbitPublisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open publish channel: %w", err)
	}

	for _, exchange := range exchanges {
		if err := ch.ExchangeDeclare(exchange.Name, exchange.Kind, true, false, false, false, nil); err != nil {
			ch.Close()
			return nil, fmt.Errorf("declare exchange %s: %w", exchange.Name, err)
		}
	}

	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, fmt.Errorf("enable publisher confirms: %w", err)
	}

	return &RabbitPublisher{ch: ch, returns: ch.NotifyReturn(make(chan amqp091.Return, 1))}, nil
}

func (p *RabbitPublisher) Close() error {
	if p == nil || p.ch == nil {
		return nil
	}
	return p.ch.Close()
}

func (p *RabbitPublisher) PublishJSON(ctx context.Context, exchange, routingKey string, payload any) error {
	return p.publishJSON(ctx, exchange, routingKey, payload, false)
}

func (p *RabbitPublisher) PublishProgressJSON(ctx context.Context, exchange, routingKey string, payload any) error {
	return p.publishJSON(ctx, exchange, routingKey, payload, true)
}

func (p *RabbitPublisher) publishJSON(ctx context.Context, exchange, routingKey string, payload any, mandatory bool) error {
	if p == nil || p.ch == nil {
		return errors.New("rabbitmq publish channel is not initialized")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	messageID := fmt.Sprintf("%d", time.Now().UTC().UnixNano())
	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, mandatory, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		Timestamp:    time.Now().UTC(),
		MessageId:    messageID,
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("publish rabbitmq event: %w", err)
	}
	if confirmation == nil {
		return errors.New("publisher confirmation was not created")
	}

	acked, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("wait for publisher confirmation: %w", err)
	}
	if !acked {
		return errors.New("rabbitmq publisher negatively acknowledged the event")
	}
	if mandatory {
		select {
		case returned := <-p.returns:
			log.Printf("ERROR unroutable RabbitMQ progress event exchange=%s routingKey=%s replyCode=%d replyText=%s messageId=%s", exchange, routingKey, returned.ReplyCode, returned.ReplyText, returned.MessageId)
			return fmt.Errorf("rabbitmq returned unroutable progress event: %d %s", returned.ReplyCode, returned.ReplyText)
		default:
		}
	}

	return nil
}
