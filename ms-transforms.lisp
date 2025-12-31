;;;; ms-transforms.lisp
;;;; 自定义 pgloader 转换函数：毫秒级 Unix 时间戳转换为 timestamptz
;;;;
;;;; 使用方法：
;;;;   pgloader --load-lisp-file ms-transforms.lisp config.load

(in-package #:pgloader.transforms)

(defun unix-timestamp-ms-to-timestamptz (unixtime-ms-string)
  "Takes a unix timestamp in milliseconds and converts it to timestamptz.
   Returns NULL for nil, 0, or negative values (representing 'not set').
   Preserves millisecond precision."
  (when unixtime-ms-string
    (let ((unixtime-ms (ensure-parse-integer unixtime-ms-string)))
      (when (and unixtime-ms (> unixtime-ms 0))
        (let* ((unix-universal-diff (load-time-value
                                      (encode-universal-time 0 0 0 1 1 1970 0)))
               (seconds (floor unixtime-ms 1000))
               (milliseconds (mod unixtime-ms 1000)))
          (multiple-value-bind
            (second minute hour date month year)
            (decode-universal-time (+ seconds unix-universal-diff) 0)
            (format nil
                    "~d-~2,'0d-~2,'0d ~2,'0d:~2,'0d:~2,'0d.~3,'0dZ"
                    year month date hour minute second milliseconds)))))))

(defun unix-timestamp-ms-to-timestamptz-allow-zero (unixtime-ms-string)
  "Takes a unix timestamp in milliseconds and converts it to timestamptz.
   Allows 0 value (converts to 1970-01-01), returns NULL only for nil or negative.
   Preserves millisecond precision."
  (when unixtime-ms-string
    (let ((unixtime-ms (ensure-parse-integer unixtime-ms-string)))
      (when (and unixtime-ms (>= unixtime-ms 0))
        (let* ((unix-universal-diff (load-time-value
                                      (encode-universal-time 0 0 0 1 1 1970 0)))
               (seconds (floor unixtime-ms 1000))
               (milliseconds (mod unixtime-ms 1000)))
          (multiple-value-bind
            (second minute hour date month year)
            (decode-universal-time (+ seconds unix-universal-diff) 0)
            (format nil
                    "~d-~2,'0d-~2,'0d ~2,'0d:~2,'0d:~2,'0d.~3,'0dZ"
                    year month date hour minute second milliseconds)))))))
