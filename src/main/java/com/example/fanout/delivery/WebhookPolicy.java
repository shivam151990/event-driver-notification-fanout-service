package com.example.fanout.delivery;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.Locale;

/** Validates both the URI and the addresses actually used by the HTTP connection. */
public final class WebhookPolicy {
    @FunctionalInterface
    interface Resolver {
        InetAddress[] resolve(String host) throws UnknownHostException;
    }

    private final boolean allowHttp;
    private final boolean allowPrivateAddresses;
    private final Resolver resolver;

    public WebhookPolicy(boolean allowHttp, boolean allowPrivateAddresses) {
        this(allowHttp, allowPrivateAddresses, InetAddress::getAllByName);
    }

    WebhookPolicy(boolean allowHttp, boolean allowPrivateAddresses, Resolver resolver) {
        this.allowHttp = allowHttp;
        this.allowPrivateAddresses = allowPrivateAddresses;
        this.resolver = resolver;
    }

    public URI validate(String value) {
        final URI uri;
        try {
            uri = new URI(value);
        } catch (URISyntaxException | NullPointerException exception) {
            throw new IllegalArgumentException("webhookUrl must be a valid absolute URI");
        }
        String scheme = uri.getScheme() == null ? "" : uri.getScheme().toLowerCase(Locale.ROOT);
        if (!(scheme.equals("https") || allowHttp && scheme.equals("http"))) {
            throw new IllegalArgumentException("webhookUrl must use HTTPS");
        }
        if (uri.getHost() == null || uri.getHost().isBlank() || uri.getUserInfo() != null
                || uri.getFragment() != null || uri.getPort() == 0 || uri.getPort() > 65535) {
            throw new IllegalArgumentException("webhookUrl requires a host and must not contain credentials or a fragment");
        }
        return uri;
    }

    /** The returned addresses must be pinned into the HTTP client's DNS resolver. */
    public InetAddress[] validateAndResolve(URI uri) throws UnknownHostException {
        validate(uri.toString());
        InetAddress[] addresses = resolver.resolve(uri.getHost());
        if (addresses.length == 0) {
            throw new UnknownHostException("Webhook hostname resolved to no addresses");
        }
        if (!allowPrivateAddresses) {
            for (InetAddress address : addresses) {
                if (!isPublicAddress(address)) {
                    throw new IllegalArgumentException("Webhook hostname resolves to a prohibited address");
                }
            }
        }
        return addresses.clone();
    }

    static boolean isPublicAddress(InetAddress address) {
        if (address.isAnyLocalAddress() || address.isLoopbackAddress() || address.isLinkLocalAddress()
                || address.isSiteLocalAddress() || address.isMulticastAddress()) {
            return false;
        }
        byte[] bytes = address.getAddress();
        if (bytes.length == 4) {
            int a = Byte.toUnsignedInt(bytes[0]);
            int b = Byte.toUnsignedInt(bytes[1]);
            int c = Byte.toUnsignedInt(bytes[2]);
            int d = Byte.toUnsignedInt(bytes[3]);
            return !(a == 0 || a == 10 || a == 127 || a >= 224
                    || a == 100 && b >= 64 && b <= 127
                    || a == 169 && b == 254
                    || a == 172 && b >= 16 && b <= 31
                    || a == 192 && b == 0 && (c == 0 || c == 2)
                    || a == 192 && b == 88 && c == 99
                    || a == 192 && b == 168
                    || a == 198 && (b == 18 || b == 19)
                    || a == 198 && b == 51 && c == 100
                    || a == 203 && b == 0 && c == 113
                    || a == 168 && b == 63 && c == 129 && d == 16);
        }
        // Only global unicast IPv6. Also exclude tunnelling, protocol and documentation ranges.
        int a = Byte.toUnsignedInt(bytes[0]);
        int b = Byte.toUnsignedInt(bytes[1]);
        int c = Byte.toUnsignedInt(bytes[2]);
        int d = Byte.toUnsignedInt(bytes[3]);
        return (a & 0xe0) == 0x20
                && !(a == 0x20 && b == 0x01 && c <= 0x01)
                && !(a == 0x20 && b == 0x01 && c == 0x0d && d == 0xb8)
                && !(a == 0x20 && b == 0x02)
                && !(a == 0x3f && b == 0xff && (c & 0xf0) == 0);
    }
}
