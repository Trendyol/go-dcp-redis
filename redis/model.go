package redis

import "time"

type Model interface {
	Convert() *Command
}

type Command struct {
	Value     interface{}
	Operation string
	Key       string
	Args      []interface{}
	TTL       time.Duration
}

type Set struct {
	Value interface{}
	Key   string
	TTL   time.Duration
}

func (s *Set) Convert() *Command {
	return &Command{
		Operation: "SET",
		Key:       s.Key,
		Value:     s.Value,
		TTL:       s.TTL,
	}
}

type Del struct {
	Key string
}

func (d *Del) Convert() *Command {
	return &Command{
		Operation: "DEL",
		Key:       d.Key,
	}
}

type HSet struct {
	Value interface{}
	Key   string
	Field string
	TTL   time.Duration
}

func (h *HSet) Convert() *Command {
	return &Command{
		Operation: "HSET",
		Key:       h.Key,
		Args:      []interface{}{h.Field, h.Value},
		TTL:       h.TTL,
	}
}

type HDel struct {
	Key   string
	Field string
}

func (h *HDel) Convert() *Command {
	return &Command{
		Operation: "HDEL",
		Key:       h.Key,
		Args:      []interface{}{h.Field},
	}
}

type Raw struct {
	Value     interface{}
	Operation string
	Key       string
	Args      []interface{}
	TTL       time.Duration
}

func (r *Raw) Convert() *Command {
	return &Command{
		Operation: r.Operation,
		Key:       r.Key,
		Value:     r.Value,
		TTL:       r.TTL,
		Args:      r.Args,
	}
}
