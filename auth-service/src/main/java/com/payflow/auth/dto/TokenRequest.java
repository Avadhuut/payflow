package com.payflow.auth.dto;

import lombok.Data;

@Data
public class TokenRequest {
    private String refreshToken;
}
