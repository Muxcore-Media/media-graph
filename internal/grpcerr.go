package internal

import (
	"errors"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func toGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	msg := err.Error()
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "not found") {
		return status.Error(codes.NotFound, msg)
	}
	if strings.Contains(lower, "required") || strings.Contains(lower, "must not") ||
		strings.Contains(lower, "must be") || strings.Contains(lower, "invalid") {
		return status.Error(codes.InvalidArgument, msg)
	}
	if errors.Is(err, errNotFound) {
		return status.Error(codes.NotFound, msg)
	}
	return status.Error(codes.Internal, msg)
}
