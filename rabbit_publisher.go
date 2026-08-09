package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type ExchangeSpec struct {
	Name string
	Kind string
}

type RabbitPublisher struct {
	ch *amqp091.Channel
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

	return &RabbitPublisher{ch: ch}, nil
}

func (p *RabbitPublisher) Close() error {
	if p == nil || p.ch == nil {
		return nil
	}
	return p.ch.Close()
}

func (p *RabbitPublisher) PublishJSON(ctx context.Context, exchange, routingKey string, payload any) error {
	if p == nil || p.ch == nil {
		return errors.New("rabbitmq publish channel is not initialized")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal event payload: %w", err)
	}

	confirmation, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, amqp091.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp091.Persistent,
		Timestamp:    time.Now().UTC(),
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

	return nil
}
