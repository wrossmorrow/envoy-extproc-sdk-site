package com.github.wrossmorrow.extproc.demo;

import com.github.wrossmorrow.extproc.ExternalProcessorServer;
import com.github.wrossmorrow.extproc.RequestProcessor;
import com.github.wrossmorrow.extproc.ServerOptions;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Entrypoint for the Java demo extproc. Serves the envoy external processor gRPC service with the
 * {@link DemoRequestProcessor} demo processor and the Prometheus metrics endpoint.
 */
public class DemoExtProcServer {

  public static final int DEFAULT_EXTPROC_PORT = 50051;
  public static final int DEFAULT_METRICS_PORT = 9090;

  /**
   * Start an {@link ExternalProcessorServer} serving the demo processor on the given extproc port,
   * e.g. for tests and embedding. Runs the same wiring as {@link
   * ExternalProcessorServer#mustServe}.
   */
  public static ExternalProcessorServer startServer(int port, RequestProcessor processor)
      throws IOException {
    ExternalProcessorServer server = new ExternalProcessorServer().builder(port);
    server.addRequestProcessor(processor);
    server.start();
    return server;
  }

  public static void main(String[] args) {
    ExternalProcessorServer.mustServe(
        ServerOptions.defaultOptions(),
        new DemoRequestProcessor(),
        Logger.getLogger(DemoExtProcServer.class.getName()));
  }
}
