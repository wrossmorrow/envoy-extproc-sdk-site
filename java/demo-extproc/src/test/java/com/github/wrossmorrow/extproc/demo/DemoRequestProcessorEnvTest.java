package com.github.wrossmorrow.extproc.demo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.wrossmorrow.extproc.ProcessingOptions;
import org.junit.jupiter.api.Test;

class DemoRequestProcessorEnvTest {

  private static final class EnvStubProcessor extends DemoRequestProcessor {
    private String controllerAddr;
    private Long exchangeTimeoutSeconds;
    private Boolean abortOnProcessorFailure;

    EnvStubProcessor(
        String controllerAddr, Long exchangeTimeoutSeconds, Boolean abortOnProcessorFailure) {
      super(controllerAddr);
      this.controllerAddr = controllerAddr;
      this.exchangeTimeoutSeconds = exchangeTimeoutSeconds;
      this.abortOnProcessorFailure = abortOnProcessorFailure;
    }

    @Override
    protected String controllerAddr() {
      return controllerAddr;
    }

    @Override
    protected long exchangeTimeoutSeconds() {
      return exchangeTimeoutSeconds == null
          ? DemoRequestProcessor.DEFAULT_EXCHANGE_TIMEOUT_SECONDS
          : exchangeTimeoutSeconds;
    }

    @Override
    protected boolean abortOnProcessorFailure() {
      return abortOnProcessorFailure == null
          ? DemoRequestProcessor.abortOnProcessorFailureFromEnv()
          : abortOnProcessorFailure;
    }

    @Override
    public ProcessingOptions getOptions() {
      ProcessingOptions opts = new ProcessingOptions();
      opts.abortOnProcessorFailure = abortOnProcessorFailure;
      return opts;
    }
  }

  @Test
  void controllerAddressDefaultsToControllerServiceName() {
    assertEquals("controller:9093", DemoRequestProcessor.controllerAddrFromEnv());
  }

  @Test
  void controllerAddressDefaultsWhenEnvUnsetOrEmpty() {
    assertEquals("controller:9093", new DemoRequestProcessor("127.0.0.1:1234").controllerAddr());
  }

  @Test
  void exchangeTimeoutDefaultsToFiveMinutes() {
    assertEquals(300L, DemoRequestProcessor.exchangeTimeoutSecondsFromEnv());
  }

  @Test
  void exchangeTimeoutDefaultsWhenEnvUnsetOrEmpty() {
    assertEquals(300L, new DemoRequestProcessor("127.0.0.1:1234").exchangeTimeoutSeconds());
  }

  @Test
  void abortOnProcessorFailureDefaultsToTrue() {
    assertTrue(DemoRequestProcessor.abortOnProcessorFailureFromEnv());
  }

  @Test
  void optionsAbortOnProcessorFailureFollowsEnvDefault() {
    DemoRequestProcessor processor = new DemoRequestProcessor("127.0.0.1:1234");
    assertEquals(Boolean.TRUE, processor.getOptions().abortOnProcessorFailure);
  }

  @Test
  void optionsAbortOnProcessorFailureFollowsOverriddenEnv() {
    EnvStubProcessor processor = new EnvStubProcessor("127.0.0.1:1234", 42L, Boolean.FALSE);
    assertEquals(Boolean.FALSE, processor.getOptions().abortOnProcessorFailure);
  }

  @Test
  void optionsExposeProcessorNameAndAbortFlag() {
    ProcessingOptions options = new DemoRequestProcessor("127.0.0.1:1234").getOptions();
    assertEquals(Boolean.TRUE, options.abortOnProcessorFailure);
  }

  @Test
  void shutdownIsSafeToCallTwice() {
    DemoRequestProcessor processor = new DemoRequestProcessor("127.0.0.1:1234");
    processor.shutdown();
    processor.shutdown();
  }
}
