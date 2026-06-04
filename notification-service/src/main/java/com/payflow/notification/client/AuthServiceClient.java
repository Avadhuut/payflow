package com.payflow.notification.client;

import com.payflow.notification.dto.UserDto;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.UUID;

@FeignClient(name = "auth-service")
public interface AuthServiceClient {

    @GetMapping("/api/auth/{id}")
    UserDto getUser(@PathVariable("id") UUID id);
}
