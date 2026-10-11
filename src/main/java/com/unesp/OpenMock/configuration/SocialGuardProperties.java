package com.unesp.OpenMock.configuration;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("openmock.socialguard")
public record SocialGuardProperties(String baseUrl, Duration connectTimeout, Duration readTimeout) {
}
