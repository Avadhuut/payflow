package com.payflow.auth.controller;

import com.payflow.auth.dto.LoginRequest;
import com.payflow.auth.dto.RegisterRequest;
import com.payflow.auth.dto.TokenRequest;
import com.payflow.auth.entity.User;
import com.payflow.auth.service.AuthService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

    private final AuthService authService;

    public AuthController(AuthService authService) {
        this.authService = authService;
    }

    @PostMapping("/register")
    public ResponseEntity<?> register(@RequestBody RegisterRequest request) {
        User user = authService.registerUser(request.getEmail(), request.getPassword(), request.getRole());
        return ResponseEntity.ok(Map.of(
                "message", "User registered successfully",
                "userId", user.getId().toString()
        ));
    }

    @PostMapping("/login")
    public ResponseEntity<?> login(@RequestBody LoginRequest request) {
        Map<String, String> tokens = authService.login(request.getEmail(), request.getPassword());
        return ResponseEntity.ok(tokens);
    }

    @PostMapping("/refresh")
    public ResponseEntity<?> refresh(@RequestBody TokenRequest request) {
        Map<String, String> tokens = authService.refresh(request.getRefreshToken());
        return ResponseEntity.ok(tokens);
    }

    @PostMapping("/logout")
    public ResponseEntity<?> logout(@RequestBody TokenRequest request) {
        authService.logout(request.getRefreshToken());
        return ResponseEntity.ok(Map.of("message", "User logged out successfully"));
    }

    @GetMapping("/{id}")
    public ResponseEntity<?> getUser(@PathVariable java.util.UUID id) {
        return authService.getUserById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
