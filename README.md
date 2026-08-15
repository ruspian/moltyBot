# Claw Royale Bot

Satu agent, satu wallet — sesuai aturan platform (`1 SC wallet = 1 agent aktif
per entryType`, dan in-game teaming/multi-account terdeteksi & dihukum).
Bot ini fokus main **satu** agent sebaik mungkin: prioritas bertahan hidup
(survival time adalah metrik ranking utama sejak v1.15.0, kills cuma
tiebreak), baru cari fight kalau aman.

## Isi folder

- `bot.py` — bot utama (REST + WebSocket game loop)
- `Dockerfile`, `docker-compose.yml` — untuk jalan 24/7 di Docker
- `requirements.txt` — dependency Python
- `.env.example` — template config, isi API key kamu di sini

## Cara pakai di server Proxmox (Ubuntu Server + Docker)

1. Copy folder ini ke server, misal via `scp` atau `git clone` kalau kamu push ke repo sendiri:
   ```bash
   scp -r clawroyale-bot/ user@ip-server-kamu:/opt/
   ssh user@ip-server-kamu
   cd /opt/clawroyale-bot
   ```

2. Siapkan file `.env`:
   ```bash
   cp .env.example .env
   nano .env   # isi CLAW_API_KEY dengan API key kamu (mr_live_...)
   ```

3. Build & jalankan:
   ```bash
   docker compose up -d --build
   ```

4. Cek log realtime:
   ```bash
   docker compose logs -f
   ```

5. Berhenti / restart:
   ```bash
   docker compose stop
   docker compose restart
   docker compose down       # stop + hapus container (image tetap ada)
   ```

Dengan `restart: unless-stopped` di compose, container otomatis nyala lagi
kalau Docker daemon restart atau servernya reboot — jadi ini sudah "jalan
24 jam" tanpa perlu setup systemd tambahan.

## Sebelum dijalankan: pastikan loadout kamu

Bot akan coba otomatis mengisi loadout (Main+Sub pack + 3 relic) dari
inventory yang **sudah kamu punya** — tapi dia **tidak membeli apa pun** di
shop. Kalau akun kamu masih kosong (fresh account), main dulu manual lewat
web buat klaim onboarding bundle (`WELCOME` — kode redeem, sekali per akun)
dan/atau beli beberapa pack/relic dasar di shop, supaya bot tidak masuk game
dengan base stats doang.

## Yang perlu kamu tahu soal strategi bot ini

- **Survive-first**: keluar dari death zone dan mundur saat HP < 35% adalah
  prioritas tertinggi — mengalahkan cari kill.
  ranking sekarang: alive → survival time → kills → EP used → agent id.
- **Fight selektif**: hanya menyerang agent lain kalau HP kita sehat (≥60%)
  DAN targetnya lebih lemah dan sudah terlihat di region yang sama (tidak
  mengejar).
- **Ruin exploration**: dieksplorasi kalau alert belum aktif dan HP aman.
- **Talk/whisper**: hook sudah disiapkan di `maybe_act()` tapi belum diisi
  logic — ini free action (tidak makan giliran/cooldown), bisa kamu
  kembangkan sendiri kalau mau agent-nya "ngobrol" juga.

## Catatan jujur soal skema action WebSocket

Dokumentasi publik yang saya akses menjelaskan jenis frame (`agent_view`,
`turn_advanced`, `action_result`, `can_act_changed`, `agent_died`, dst) dan
aturan umum (cooldown-group vs free actions, kapan boleh kirim action lagi)
dengan cukup detail. Tapi skema **field-level eksak** untuk payload
`action` per jenis aksi (`move`/`attack`/`explore`/dll — nama field target
region/agent/monster/ruin) tidak sepenuhnya saya dapatkan dari sumber yang
bisa saya akses.

`build_action_payload()` di `bot.py` memakai bentuk payload yang paling
masuk akal berdasarkan pola yang ada (`{"type": "action", "action": "move",
"targetRegionId": "..."}` dst), dan **setiap `action_result` dari server
di-log lengkap** (`log.info(... "error=%s" ...)`) supaya kamu bisa lihat
langsung field mana yang diterima/ditolak server dan tinggal sesuaikan
`build_action_payload()` sesuai itu — cek juga `references/actions.md` di
skill docs kamu (atau `/openapi.yaml` versi lengkap / Swagger UI di `/docs`)
kalau butuh kepastian sebelum runtime.

## Kenapa cuma satu bot, bukan banyak

Claw Royale sendiri membatasi ini di level sistem (satu SC wallet cuma bisa
punya satu agent aktif per entryType, dan hanya "primary agent" yang boleh
main), dan secara eksplisit mendeteksi & menghukum in-game teaming. Jalanin
banyak akun/bot dari satu operator untuk membanjiri room yang sama demi
memperbesar peluang menang melanggar aturan fair-play platform ini — jadi
setup ini sengaja dibuat untuk satu agent yang solid, bukan sekumpulan bot.
