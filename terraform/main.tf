resource "aws_s3_bucket" "raw" {
  bucket        = "scaling-raw"
  force_destroy = true
}

resource "aws_s3_bucket" "refined" {
  bucket        = "scaling-refined"
  force_destroy = true
}

resource "aws_sqs_queue" "dlq" {
  name                      = "ingestion-dlq"
  message_retention_seconds = 86400
}

resource "aws_sqs_queue" "ingestion_queue" {
  name                       = "ingestion-queue"
  visibility_timeout_seconds = 300
  message_retention_seconds  = 86400

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 3
  })
}

resource "aws_sqs_queue" "processed_events" {
  name                       = "processed-events-queue"
  visibility_timeout_seconds = 60
  message_retention_seconds  = 86400
}
