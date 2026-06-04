package com.payflow.auth.service;

import com.payflow.auth.entity.Role;
import com.payflow.auth.entity.User;
import com.payflow.auth.repository.UserRepository;
import com.payflow.auth.util.JwtUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class AuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtUtils jwtUtils;
    private final StringRedisTemplate redisTemplate;
    private final long refreshExpirationSeconds;

    public AuthService(
            UserRepository userRepository,
            PasswordEncoder passwordEncoder,
            JwtUtils jwtUtils,
            StringRedisTemplate redisTemplate,
            @Value("${app.jwt.refresh-expiration}") long refreshExpirationSeconds) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        this.jwtUtils = jwtUtils;
        this.redisTemplate = redisTemplate;
        this.refreshExpirationSeconds = refreshExpirationSeconds;
    }

    private String getRedisKey(String token) {
        return "auth:refresh:" + token;
    }

    @Transactional
    public User registerUser(String email, String password, Role role) {
        if (userRepository.findByEmail(email).isPresent()) {
            throw new IllegalArgumentException("Email is already registered");
        }
        User user = User.builder()
                .email(email)
                .password(passwordEncoder.encode(password))
                .role(role)
                .build();
        return userRepository.save(user);
    }

    @Transactional
    public Map<String, String> login(String email, String password) {
        User user = userRepository.findByEmail(email)
                .orElseThrow(() -> new IllegalArgumentException("Invalid email or password"));

        if (!passwordEncoder.matches(password, user.getPassword())) {
            throw new IllegalArgumentException("Invalid email or password");
        }

        String accessToken = jwtUtils.generateToken(user.getId(), user.getEmail(), user.getRole());
        String refreshToken = UUID.randomUUID().toString();

        // Save refresh token to Redis with 7 day TTL
        redisTemplate.opsForValue().set(
                getRedisKey(refreshToken),
                user.getId().toString(),
                refreshExpirationSeconds,
                TimeUnit.SECONDS
        );

        Map<String, String> tokens = new HashMap<>();
        tokens.put("accessToken", accessToken);
        tokens.put("refreshToken", refreshToken);
        return tokens;
    }

    @Transactional
    public Map<String, String> refresh(String refreshToken) {
        String userIdStr = redisTemplate.opsForValue().get(getRedisKey(refreshToken));
        if (userIdStr == null) {
            throw new IllegalArgumentException("Invalid or expired refresh token");
        }

        UUID userId = UUID.fromString(userIdStr);
        User user = userRepository.findById(userId)
                .orElseThrow(() -> new IllegalArgumentException("User not found"));

        String newAccessToken = jwtUtils.generateToken(user.getId(), user.getEmail(), user.getRole());
        String newRefreshToken = UUID.randomUUID().toString();

        // Rotate Refresh Token
        redisTemplate.delete(getRedisKey(refreshToken));
        redisTemplate.opsForValue().set(
                getRedisKey(newRefreshToken),
                user.getId().toString(),
                refreshExpirationSeconds,
                TimeUnit.SECONDS
        );

        Map<String, String> tokens = new HashMap<>();
        tokens.put("accessToken", newAccessToken);
        tokens.put("refreshToken", newRefreshToken);
        return tokens;
    }

    @Transactional
    public void logout(String refreshToken) {
        redisTemplate.delete(getRedisKey(refreshToken));
    }

    @Transactional(readOnly = true)
    public java.util.Optional<User> getUserById(UUID id) {
        return userRepository.findById(id);
    }
}
