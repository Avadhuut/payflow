package com.payflow.auth.dto;

import com.payflow.auth.entity.Role;
import lombok.Data;

@Data
public class RegisterRequest {
    private String email;
    private String password;
    private Role role;
}
