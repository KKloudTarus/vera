"""Object-store adapters (S3-compatible: MinIO/Ceph/any). Vendor-neutral."""

from vera.adapters.objectstore.s3_adapter import S3ObjectStore

__all__ = ["S3ObjectStore"]
