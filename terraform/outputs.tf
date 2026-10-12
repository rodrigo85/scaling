output "raw_bucket" {
  value       = aws_s3_bucket.raw.bucket
  description = "Bucket S3 para dados brutos"
}

output "refined_bucket" {
  value       = aws_s3_bucket.refined.bucket
  description = "Bucket S3 para dados refinados (Parquet/Delta)"
}

output "sqs_queue_url" {
  value       = aws_sqs_queue.ingestion_queue.url
  description = "URL da fila SQS para envio de mensagens"
}

output "sqs_queue_arn" {
  value       = aws_sqs_queue.ingestion_queue.arn
  description = "ARN da fila SQS"
}

output "processed_events_queue_url" {
  value       = aws_sqs_queue.processed_events.url
  description = "URL da fila de saida (eventos processados downstream)"
}
