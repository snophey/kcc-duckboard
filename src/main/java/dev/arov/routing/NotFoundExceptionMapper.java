package dev.arov.routing;

import jakarta.inject.Inject;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriInfo;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

import java.io.InputStream;
import java.util.Optional;

/**
 * Redirects to the index.html page when a 404 error occurs. This achieves SPA routing.
 */
@Provider
public class NotFoundExceptionMapper implements ExceptionMapper<NotFoundException> {
    @Inject
    UriInfo uriInfo;

    private Optional<String> getUriExtension() {
        String path = uriInfo.getPath();
        int lastDot = path.lastIndexOf('.');
        int lastSlash = path.lastIndexOf('/');
        if (lastDot == -1 || lastSlash > lastDot) {
            // the path has no extension since the last segment after the last slash has no dot
            return Optional.empty();
        }
        return Optional.of(path.substring(lastDot).toLowerCase());
    }

    @Override
    @Produces(MediaType.TEXT_HTML)
    public Response toResponse(NotFoundException exception) {
        if (getUriExtension().filter(ext -> !ext.equals(".html")).isPresent()) {
            // if any extension other than .html is requested, return a 404 error
            // since it makes no sense to serve a html page when a different kind of resource is requested
            return Response.status(Response.Status.NOT_FOUND).build();
        }
        // serve the index.html page when a 404 error occurs (and let the client-side router handle the routing)
        InputStream resource = getClass().getResourceAsStream("/META-INF/resources/index.html");
        return null == resource
                ? Response.status(Response.Status.NOT_FOUND).build()
                : Response.ok().entity(resource).build();
    }
}
