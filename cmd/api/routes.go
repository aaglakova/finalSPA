package main

import (
	"expvar"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

func (app *application) routes() http.Handler {
	// Instance of httprouter router.
	router := httprouter.New()

	// Convert notFoundResponse() helper to a http.Handler
	router.NotFound = http.HandlerFunc(app.notFoundResponse)
	router.MethodNotAllowed = http.HandlerFunc(app.methodNotAllowedResponse)

	// URL patterns and Handler Functions
	router.HandlerFunc(http.MethodGet, "/v1/healthcheck", app.healthcheckHandler)

	router.HandlerFunc(http.MethodPost, "/v1/books", app.requirePermission("books:write", app.createBooksHandler))
	router.HandlerFunc(http.MethodGet, "/v1/books", app.requirePermission("books:read", app.listBooksHandler))
	router.HandlerFunc(http.MethodGet, "/v1/books/:id", app.requirePermission("books:read", app.showBooksHandler))
	router.HandlerFunc(http.MethodPatch, "/v1/books/:id", app.requirePermission("books:write", app.updateBooksHandler))
	router.HandlerFunc(http.MethodDelete, "/v1/books/:id", app.requirePermission("books:write", app.deleteBooksHandler))

	router.HandlerFunc(http.MethodPost, "/v1/users", app.registerUserHandler)
	router.HandlerFunc(http.MethodPut, "/v1/users/activated", app.activateUserHandler)

	router.HandlerFunc(http.MethodPost, "/v1/tokens/authentication", app.createAuthenticationTokenHandler)

	router.Handler(http.MethodGet, "/debug/vars", expvar.Handler())

	return app.metrics(app.recoverPanic(app.enableCORS(app.rateLimit(app.authenticate(router)))))
}
