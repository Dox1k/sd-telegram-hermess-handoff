# tg-projects — Telegram-интерфейс для Hermes

Один чат (или форум-топик) = один проект + одна активная сессия. Панель с
inline-кнопками, бесшовный sync Telegram ↔ десктоп, approve/deny опасных
команд кнопками, мастер создания проектов. Работает поверх Hermes-платформы
как плагин — НЕ самостоятельный бот.

## Модель

- Flat-чат (без топиков): один активный ход на чат; переключение сессии во
  время хода откладывается, следующее сообщение уходит уже в выбранную.
- `/menu` — панель: проект / сессия (живой дайджест: сообщения, токены,
  модель) / новая сессия / stop / ещё. Никогда не прерывает идущий ход.
- Выбор проекта автоматически входит в его последнюю сессию (сводка).
- Свободный текст идёт в активную сессию чата; непривязанный чат получает
  «Сначала выбери проект» + панель.
- Ответы с десктопа зеркалятся в Telegram (без дублей TG-ходов).
- Опасные команды: кнопки [✅ Одобрить] [❌ Отклонить] в чате, fail-closed.

## Установка (на машине получателя)

1. Установи Hermes (`hermes-agent`) и запусти gateway.
2. Склонируй плагин в `~/.hermes/plugins/tg-projects/`:
   ```
   git clone https://github.com/Dox1k/sd-telegram-hermess-handoff.git ~/.hermes/plugins/tg-projects
   ```
   Обновления потом: `git -C ~/.hermes/plugins/tg-projects pull`.
3. Создай своего бота у @BotFather, токен положи в `~/.hermes/.env`:
   ```
   TELEGRAM_BOT_TOKEN=<токен>
   ```
4. Свой Telegram user id (узнать у @userinfobot) укажи в конфиге и окружении:
   ```
   # ~/.hermes/config.yaml
   platforms:
     telegram:
       extra:
         allow_admin_from: ["<твой_id>"]
   security:
     approval:
       transport: "tg-topics"
       transport_fallback: "deny"
   approvals:
     mode: "smart"
   ```
   ```
   # окружение gateway (или ~/.hermes/.env)
   TGP_OWNER_CHAT=<твой_id>
   ```
5. Перезапусти gateway. В чате с ботом: `/menu`.

## Проверка

```
TGP_PLUGIN_DIR=~/.hermes/plugins/tg-projects python3 -m pytest <repo> -q
```
270 тестов должны быть зелёными.

## Заметки

- Whitelist: только владелец (TGP_OWNER_CHAT); чужие — drop.
- Рестарт gateway: `kill -USR1 <pid>`; перед рестартом убедись, что в
  `state.db.session_turn_leases` нет активных чужих ходов.
- Команды BotFather пушатся автоматически (chat-scope владелька).
- История плагина: см. `git log` — модель «handoff Yes/No» удалена,
  seamless sync вместо неё.
- `state.json` (привязки топиков, локальное состояние) в репозиторий не
  входит: создаётся при работе, `.gitignore`.
