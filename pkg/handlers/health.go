package handlers

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"
	apiv1 "github.com/elee1766/gobtr/gen/api/v1"
	"github.com/elee1766/gobtr/pkg/db"
)

type HealthHandler struct {
	logger *slog.Logger
	db     *db.DB
}

func NewHealthHandler(logger *slog.Logger, db *db.DB) *HealthHandler {
	return &HealthHandler{
		logger: logger.With("handler", "health"),
		db:     db,
	}
}

func (h *HealthHandler) Check(
	ctx context.Context,
	req *connect.Request[apiv1.HealthCheckRequest],
) (*connect.Response[apiv1.HealthCheckResponse], error) {
	h.logger.Debug("health check", "service", req.Msg.Service)

	// TODO: Add actual health checks
	return connect.NewResponse(&apiv1.HealthCheckResponse{
		Status:  apiv1.HealthCheckResponse_SERVING,
		Message: "service is healthy",
	}), nil
}
