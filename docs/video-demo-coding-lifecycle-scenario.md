# Demo setup Vera trên ba coding tool

Mục tiêu của demo là dùng cùng một project baseline và cùng **một prompt** để:

1. Khởi động Vera local stack.
2. Cài Vera ở project scope.
3. Nạp behavior skill và hook/plugin đúng với coding tool hiện tại.
4. Smoke test hai endpoint API và MCP.

Ba runtime được kiểm tra độc lập: OpenCode, Codex và Claude Code.

## Điều kiện

- Docker Desktop và Docker Compose hoạt động.
- `opencode`, `codex` và `claude` đã được cài.
- Cả ba CLI đã đăng nhập vào model provider; kiểm tra trước bằng lệnh status tương ứng.
- Git có trong `PATH`; Claude Code và Codex cần thêm Node.js.
- Vera source checkout có file `docker-compose.yml` và thư mục
  `examples/integrations/` của branch đang test.
- Baseline là một Git repository có đúng một remote an toàn. Nó có thể chưa có Vera config hoặc
  chứa integration cũ mà demo chủ động thay thế.
- Không đặt token hoặc secret trong baseline, prompt hay file được commit.

## Khởi động Vera

Khởi động stack một lần trước khi đo setup của ba tool:

```powershell
$VeraRepo = Resolve-Path "C:\path\to\vera"
Set-Location $VeraRepo
docker compose --profile app up --build -d
curl.exe --fail --silent http://localhost:8000/health/live
curl.exe --fail --silent http://localhost:8000/health/ready
```

## Dupe baseline

Chạy PowerShell một lần. Thay hai path đầu tiên bằng path thật:

```powershell
$VeraRepo = Resolve-Path "C:\path\to\vera"
$Baseline = Resolve-Path "C:\path\to\clean-baseline-project"
$DemoRoot = Join-Path $env:TEMP "vera-three-tools-demo"

if (Test-Path -LiteralPath $DemoRoot) {
    throw "Demo root already exists: $DemoRoot"
}

New-Item -ItemType Directory -Path $DemoRoot | Out-Null
foreach ($Runtime in @("opencode", "codex", "claude-code")) {
    $Target = Join-Path $DemoRoot $Runtime
    Copy-Item -Recurse -LiteralPath $Baseline -Destination $Target
    Copy-Item -Recurse -LiteralPath (Join-Path $VeraRepo "examples\integrations") `
        -Destination (Join-Path $Target ".vera-baseline")
}
```

`.vera-baseline/` là input tạm cho agent, không phải artifact cần commit vào project demo.
Mỗi bản sao bắt đầu từ cùng source, Git remote và Vera integration baseline nhưng nhận config
riêng của runtime đang chạy.

## Prompt duy nhất

Paste nguyên prompt này vào cả ba coding tool, không thêm runtime-specific instruction:

```text
Set up Vera for this project.

Run `.vera-baseline/vera-project-setup/SKILL.md` with:

VERA_API_URL=http://localhost:8000
VERA_MCP_URL=http://localhost:8080/mcp

Detect the current coding tool and apply its matching project-local runtime spec. Replace stale
Vera integration files with the canonical config, skill, and hook or plugin. I approve that
project-local setup diff.

Smoke test the API and MCP URLs and validate the installed files. Then ask me to restart the
coding tool and return to this setup session. When I return, confirm the `vera` MCP server is
connected and its tools are visible, then report `VERA setup completed for <runtime>` and list
the changed files.
```

## Chạy ba tool

Mở ba terminal riêng, đặt working directory vào đúng bản sao rồi khởi động tool:

```powershell
$DemoRoot = Join-Path $env:TEMP "vera-three-tools-demo"; Set-Location (Join-Path $DemoRoot "opencode"); opencode
$DemoRoot = Join-Path $env:TEMP "vera-three-tools-demo"; Set-Location (Join-Path $DemoRoot "codex"); codex
$DemoRoot = Join-Path $env:TEMP "vera-three-tools-demo"; Set-Location (Join-Path $DemoRoot "claude-code"); claude
```

Paste cùng prompt ở trên vào từng session. Khi agent yêu cầu, restart tool rồi resume đúng
session setup để agent xác nhận MCP `vera`.

## Kết quả cần thấy

| Runtime | Project artifacts chính | Kiểm tra setup |
|---|---|---|
| OpenCode | `opencode.json`, `.opencode/plugins/vera.ts`, `.opencode/skills/vera-memory/SKILL.md` | Parse config, load plugin và `opencode mcp list` thấy `vera` |
| Codex | `.codex/config.toml`, `.codex/hooks.json`, `.codex/hooks/vera-hook.cjs`, `.agents/skills/vera-memory/SKILL.md` | Parse config, `node --check`, và `codex mcp get vera --json` |
| Claude Code | `.mcp.json`, `.claude/settings.json`, `.claude/hooks/vera-hook.cjs`, `.claude/skills/vera-memory/SKILL.md` | Parse config, `node --check`, và `claude mcp get vera` |

Mỗi run phải cho thấy:

- Hook/plugin chỉ inject sanitized repository và optional branch, không gọi network trực tiếp.
- MCP server name là `vera`, behavior skill được load, và không có config user/global mới.
- API health trả thành công và MCP URL trả một HTTP response hợp lệ dưới 500.
- Sau restart, runtime báo MCP `vera` connected và hiển thị tools.

Sau demo có thể dừng stack từ Vera source checkout bằng `docker compose --profile app down`.
