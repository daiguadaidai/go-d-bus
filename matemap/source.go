package matemap

type Source struct {
    Host           string
    Port           int
    Username       string
    Password       string
    Relay_log_file string
    Relay_log_pos  int64
    Start_log_file string
    Start_log_pos  int64
    Parse_log_file string
    Parse_log_pos  int64
    Stop_log_file  string
    Stop_log_pos   int64
}
