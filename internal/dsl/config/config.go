package config

type Value struct {
	Literal any
}

func Val(value any) Value {
	return Value{Literal: value}
}
