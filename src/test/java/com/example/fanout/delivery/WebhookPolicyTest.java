package com.example.fanout.delivery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.net.InetAddress;
import java.net.URI;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class WebhookPolicyTest {
    @ParameterizedTest
    @ValueSource(strings = {"http://example.com", "file:///etc/passwd", "https://user:password@example.com",
            "https://example.com/#fragment", "/relative", "https://example.com:65536", "https://example.com:0"})
    void rejectsUnsafeUris(String value) {
        assertThatIllegalArgumentException().isThrownBy(() -> new WebhookPolicy(false, false).validate(value));
    }

    @ParameterizedTest
    @ValueSource(strings = {"0.0.0.0", "10.0.0.1", "127.0.0.1", "100.64.0.1", "169.254.169.254",
            "172.16.1.1", "192.168.1.1", "192.0.0.1", "192.0.2.1", "198.18.0.1", "198.51.100.1",
            "203.0.113.1", "224.0.0.1", "255.255.255.255", "168.63.129.16", "::", "::1",
            "fe80::1", "fc00::1", "ff02::1", "2001:db8::1", "2002:7f00:1::", "64:ff9b::7f00:1",
            "::ffff:127.0.0.1", "2001::1", "3fff::1"})
    void blocksNonPublicAddresses(String value) throws Exception {
        assertThat(WebhookPolicy.isPublicAddress(InetAddress.getByName(value))).isFalse();
    }

    @ParameterizedTest
    @ValueSource(strings = {"8.8.8.8", "1.1.1.1", "93.184.216.34", "2001:4860:4860::8888", "2606:4700:4700::1111"})
    void permitsPublicAddresses(String value) throws Exception {
        assertThat(WebhookPolicy.isPublicAddress(InetAddress.getByName(value))).isTrue();
    }

    @Test
    void rejectsMixedPublicAndPrivateDnsAnswers() throws Exception {
        InetAddress[] mixed = {InetAddress.getByName("8.8.8.8"), InetAddress.getByName("127.0.0.1")};
        WebhookPolicy policy = new WebhookPolicy(false, false, host -> mixed);
        assertThatIllegalArgumentException().isThrownBy(() -> policy.validateAndResolve(URI.create("https://example.com")));
    }

    @Test
    void allowsPrivateHttpOnlyWhenBothDevelopmentOverridesAreEnabled() throws Exception {
        WebhookPolicy policy = new WebhookPolicy(true, true);
        URI uri = policy.validate("http://127.0.0.1:8089/webhook");
        assertThat(policy.validateAndResolve(uri)).containsExactly(InetAddress.getByName("127.0.0.1"));
    }
}
