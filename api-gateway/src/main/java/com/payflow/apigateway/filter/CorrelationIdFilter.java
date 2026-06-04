package com.payflow.apigateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Global filter to intercept incoming API Gateway requests, extract or generate
 * a Correlation ID (X-Correlation-ID), propagate it to downstream services,
 * and populate the logging Diagnostic Context (MDC).
 */
@Component
public class CorrelationIdFilter implements GlobalFilter, Ordered {

    private static final Logger logger = LoggerFactory.getLogger(CorrelationIdFilter.class);
    public static final String CORRELATION_ID_HEADER = "X-Correlation-ID";
    public static final String MDC_CORRELATION_ID_KEY = "correlationId";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        HttpHeaders headers = request.getHeaders();
        
        String correlationId = headers.getFirst(CORRELATION_ID_HEADER);
        
        if (correlationId == null || correlationId.trim().isEmpty()) {
            correlationId = UUID.randomUUID().toString();
            // Mutate request to inject correlation ID header
            exchange = exchange.mutate()
                    .request(request.mutate().header(CORRELATION_ID_HEADER, correlationId).build())
                    .build();
            logger.info("Generated new X-Correlation-ID: {}", correlationId);
        } else {
            logger.info("Found existing X-Correlation-ID: {}", correlationId);
        }

        // Add correlation ID to the response header for client traceability
        exchange.getResponse().getHeaders().add(CORRELATION_ID_HEADER, correlationId);

        // Put in MDC for the current reactive thread and propagate via Reactor context
        final String finalCorrelationId = correlationId;
        return chain.filter(exchange)
                .contextWrite(context -> context.put(MDC_CORRELATION_ID_KEY, finalCorrelationId))
                .doFirst(() -> MDC.put(MDC_CORRELATION_ID_KEY, finalCorrelationId))
                .doFinally(signalType -> MDC.remove(MDC_CORRELATION_ID_KEY));
    }

    @Override
    public int getOrder() {
        // Run with highest priority so correlation ID is initialized before other filters
        return Ordered.HIGHEST_PRECEDENCE;
    }
}
