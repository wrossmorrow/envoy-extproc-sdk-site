package extproc

import (
	"testing"
	"time"
)

// TestControllerAddrFromEnv verifies the controller gRPC address is read from
// the CONTROLLER_ADDR environment variable, defaulting when unset.
func TestControllerAddrFromEnv(t *testing.T) {
	t.Setenv("CONTROLLER_ADDR", "controller:9093")
	if got := controllerAddrFromEnv(); got != "controller:9093" {
		t.Errorf("controllerAddrFromEnv() = %q, want %q", got, "controller:9093")
	}

	t.Setenv("CONTROLLER_ADDR", "")
	if got := controllerAddrFromEnv(); got != defaultControllerAddr {
		t.Errorf("controllerAddrFromEnv() = %q, want default %q", got, defaultControllerAddr)
	}
}

// TestExchangeTimeoutFromEnv verifies the phase exchange timeout is read from
// the EXCHANGE_TIMEOUT environment variable, in seconds, defaulting when unset
// or invalid.
func TestExchangeTimeoutFromEnv(t *testing.T) {
	t.Setenv("EXCHANGE_TIMEOUT", "42")
	if got := exchangeTimeout(); got != 42*time.Second {
		t.Errorf("exchangeTimeout() = %v, want %v", got, 42*time.Second)
	}

	t.Setenv("EXCHANGE_TIMEOUT", "")
	if got := exchangeTimeout(); got != defaultExchangeTimeout {
		t.Errorf("exchangeTimeout() = %v, want default %v", got, defaultExchangeTimeout)
	}
}

// TestAbortOnProcessorFailureFromEnv verifies the fail-closed abort flag is
// read from the ABORT_ON_PROCESSOR_FAILURE environment variable, defaulting to
// true when unset or invalid.
func TestAbortOnProcessorFailureFromEnv(t *testing.T) {
	t.Setenv("ABORT_ON_PROCESSOR_FAILURE", "false")
	if got := abortOnProcessorFailure(); got {
		t.Errorf("abortOnProcessorFailure() = true, want false when ABORT_ON_PROCESSOR_FAILURE=false")
	}

	t.Setenv("ABORT_ON_PROCESSOR_FAILURE", "")
	if got := abortOnProcessorFailure(); !got {
		t.Errorf("abortOnProcessorFailure() = true, want default true when ABORT_ON_PROCESSOR_FAILURE is unset")
	}
}

// TestNewRequestProcessorReadsEnv verifies the demo extproc processor wires the
// phase exchange timeout and fail-closed abort flag from the environment and
// reads the controller address it is wired to.
func TestNewRequestProcessorReadsEnv(t *testing.T) {
	t.Setenv("EXCHANGE_TIMEOUT", "42")
	t.Setenv("ABORT_ON_PROCESSOR_FAILURE", "false")

	proc := NewRequestProcessor("controller:9093")
	if proc.controllerAddr != "controller:9093" {
		t.Errorf("processor controller address = %q, want %q", proc.controllerAddr, "controller:9093")
	}
	if proc.exchangeTimeout != 42*time.Second {
		t.Errorf("processor exchange timeout = %v, want %v", proc.exchangeTimeout, 42*time.Second)
	}
	if proc.opts.AbortOnProcessorFailure {
		t.Errorf("processor abort on processor failure = true, want false when ABORT_ON_PROCESSOR_FAILURE=false")
	}

	t.Setenv("EXCHANGE_TIMEOUT", "")
	t.Setenv("ABORT_ON_PROCESSOR_FAILURE", "")
	proc = NewRequestProcessor("controller:9093")
	if proc.exchangeTimeout != defaultExchangeTimeout {
		t.Errorf("processor exchange timeout = %v, want default %v", proc.exchangeTimeout, defaultExchangeTimeout)
	}
	if !proc.opts.AbortOnProcessorFailure {
		t.Errorf("processor abort on processor failure = false, want default true when ABORT_ON_PROCESSOR_FAILURE is unset")
	}
}
