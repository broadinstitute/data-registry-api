// CloudFront Function (runtime cloudfront-js-2.0), viewer-response event.
//
// Makes browsers save a download under the S3 key's base name, as the S3 presigned
// links do with ResponseContentDisposition. CloudFront has no such query parameter,
// and the responses are never cached, so this sets the header on the way out.
//
// The header value depends on the decoded base name:
//   - printable ASCII without a double quote (space to ~):  filename="<decoded name>"
//   - otherwise, when only characters outside that range are the problem (accents, CJK,
//     control characters):  filename="<raw name>"; filename*=UTF-8''<raw name>, where the
//     raw name is still percent-encoded. That is valid in a quoted string for old clients
//     and is the RFC 5987 form that browsers prefer, so they save the real name.
//   - malformed percent-encoding, or a decoded double quote:  filename="<raw name>" only.
function handler(event) {
    var response = event.response;
    if (response.statusCode < 200 || response.statusCode >= 300) {
        return response;
    }

    var uri = event.request.uri;
    var raw = uri.substring(uri.lastIndexOf('/') + 1);
    if (raw === '') {
        return response;
    }

    var value = 'attachment; filename="' + raw + '"';
    try {
        var decoded = decodeURIComponent(raw);
        if (decoded.indexOf('"') === -1) {
            if (/^[\x20-\x7E]+$/.test(decoded)) {
                value = 'attachment; filename="' + decoded + '"';
            } else {
                value += "; filename*=UTF-8''" + raw;
            }
        }
    } catch (e) {
        // Malformed percent-encoding: keep the raw name, with no filename*.
    }

    response.headers['content-disposition'] = {value: value};
    return response;
}
