-- Kiểm tra bằng chứng chất lượng dữ liệu (Trino)

-- Check nào đã FAIL / bị cảnh báo
SELECT layer, table_name, check_name, metric_value, rundate
FROM delta.control.quality_metrics WHERE passed = 'false' ORDER BY created_at DESC;

-- Dòng bị quarantine, theo lý do
SELECT table_name, failure_reason, count(*) AS n FROM delta.control.failed_records GROUP BY 1, 2 ORDER BY n DESC;

-- Trạng thái từng lần chạy
SELECT layer, table_name, status, input_rows, output_rows, rejected_rows, started_at, error_message
FROM delta.control.audit_log ORDER BY started_at DESC;
