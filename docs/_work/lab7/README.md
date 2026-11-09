# ЛР №7 – що зроблено і як здавати

Ця папка – робочий супровід ЛР№7. Сам текст пояснювальної записки живе в `docs/2_design.md` (і дублюється в
Notion), тут – логи, інструкції, промпти й скрипт збирання.

## 1. Карта: завдання → де лежить результат

| Завд. | Що вимагає умова | Де в репозиторії | Де в Notion | Бали |
| :---: | :--- | :--- | :--- | :---: |
| 1 | Обґрунтування стилю й патернів (≥ 1 стор.) + краш-тест ШІ-Техліда | `docs/2_design.md` → 2.1 (2.1.1–2.1.4) | «Розділ 2» → 2.1; лог ЛР7-1 в AI Insights | 1,25 |
| 2 | Use Case діаграма в CASE-засобі + специфікації (Pre/Happy/Post/AF/EF) + краш-тест | 2.6; модель і PNG – `docs/uml/` (**малюєте ви**, див. `staruml_guide.md`) | «Розділ 2» → 2.6; лог ЛР7-2 | 3 |
| 3 | Матриця API-маршрутів + Data Flows + краш-тест; 100 % відповідність контрактам | 2.5.3, 2.5.4; `docs/api/openapi.yaml` доповнено | «Розділ 2» → 2.5; лог ЛР7-3 | 1,25 |
| 4 | Опис серверного ядра (Inbound → Core → Outbound) + підрозділ AppSec + краш-тест | 2.3.3 і 2.4 | «Розділ 2» → 2.3, 2.4; лог ЛР7-4 | 1,25 |
| 5 | Консолідація Розділу 2 + markdownlint у GitHub Actions + зелений PR | `.markdownlint.jsonc`, `.markdownlint-cli2.jsonc`, `.github/workflows/docs-linter.yml`, `docs/F2_Diploma_Chapters1-2_Дармограй.docx` | посилання на зелений запуск + лог ЛР7-5 | 1,25 |

Нумерація Розділу 2 приведена до фінальної структури із Завдання 5: 2.1 стиль → 2.2 C4 L1–2 → 2.3 C4 L3 +
ядро (+ алгоритми А1–А3 як 2.3.4) → 2.4 AppSec → 2.5 API → 2.6 Use Cases → 2.7 дані → 2.8 аудит → висновки.
ADR-001 – у 2.1.4. Тому в умові ЛР ви бачите «2.3 AppSec» і «2.8 Use Cases» (номери з кроків 1–4), а в записці
вони стоять як 2.4 і 2.6 – так вимагає зведена структура Завдання 5. На захисті це одне речення.

## 2. Що вже зроблено автоматично

- Тексти 2.1, 2.3.3, 2.4, 2.5.3–2.5.4, 2.6, 2.8.3–2.8.4, «Висновки до розділу 2» – звірені з кодом Kurin 1.1.2
  (класи, параметри за замовчуванням, імена метрик).
- `openapi.yaml`: додано `/actuator/prometheus` і схему заголовка `X-Kurin-Consistency` (знахідки аудиту API).
- `.gitignore`: `.env`, `*.p12`, `*.jks` (знахідка аудиту AppSec).
- Лінтер: конфіги в корені, workflow – `docs-linter.yml` тут (скопіювати, розділ 4); локально `markdownlint-cli2 0.23.2` → **0 issues** у `docs/`. Попутно виправлено
  розмітку в `0_intro.md`, `1_analysis.md`, `proposal.md`, `README.md`, `paper/Kurin_tezy.md` (лише перенесення
  рядків, маркери списків, порожні рядки, `<URL>` – текст не змінювався).
- Моноліт `docs/F2_Diploma_Chapters1-2_Дармограй.docx` (78 стор., 10 рисунків) – `python docs/_work/lab7/build_monolith.py`.
- AI Insights з 5 логами: `ai_insights_lab7.md`; пітч на 6 хв: `defense_6min.md`.

## 3. Що треба зробити вам (≈ 1 година)

Порядок важливий – PR має бути зеленим з усіма файлами.

1. **Перечитати 2.1, 2.4 і 2.6.2** у `docs/2_design.md` – це те, що вас питатимуть. Кожну тезу треба вміти
   пояснити своїми словами (умова: «готовий усно захистити кожну тезу»).
2. **Прогнати промпти самостійно** (`prompts_filled.md`) – 15 хв. Умова оцінює «реальний інженерний лог». Якщо
   ваш LLM знайшов щось нове – допишіть рядок у відповідний лог (і в Notion).
3. **Намалювати діаграму в StarUML** (`staruml_guide.md`, ≈ 20 хв) і зберегти:
   `docs/uml/kurin-use-cases.mdj` і `docs/uml/kurin-use-cases.png`. Без цього Завдання 2 – максимум 1,75 з 3.
4. **Вставити PNG** у .docx (2.6.1, місце позначене жирним рядком, підпис «Рисунок 2.9» уже є) і в Notion (2.6.1,
   жовтий блок-заглушка).
5. **Закомітити й запушити** (гілку з комітом уже створено, див. розділ 4) і **відкрити PR**.
6. **Дочекатися зеленої галочки** `docs-linter` у PR → скопіювати посилання на запуск (PR → Checks → docs-linter
   → адреса сторінки) і вставити в Notion у рядок «Посилання на пайплайн» (ЛР7-5).
7. **Google Docs** (умова Завд. 5): завантажте .docx на Google Drive → «Відкрити в Google Документах» → Поділитися
   «Усі, хто має посилання – Читач». Посилання допишіть у `docs/README.md`, розділ «Зведений документ», рядком
   `Google Docs: <посилання>` і закомітьте (`docs: link the chapter 1-2 monolith in Google Docs`).
8. Двічі прорепетирувати `defense_6min.md` з таймером.

## 4. Git: як здати PR

Гілку `feature/lab7-docs-quality-gate` створено від `feature/chapter2-architecture`, перший коміт за
Conventional Commits уже є. Інші ваші незакомічені зміни (`core/`, `docs/paper/`, застейджений
`example_tezy.pdf`) **не чіпали і не включали** в коміт.

Файл конвеєра CI віддалені інструменти Claude записувати в `.github/` не можуть (захищена папка), тому його
готова копія лежить тут – `docs-linter.yml`; скопіюйте її одним рядком (PowerShell):

```powershell
cd D:\uni\KurinFrame\Kurin
git status                      # гілка feature/lab7-docs-quality-gate
New-Item -ItemType Directory -Force .github\workflows | Out-Null
Copy-Item docs\_work\lab7\docs-linter.yml .github\workflows\docs-linter.yml
git add .github/workflows/docs-linter.yml
git commit -m "ci: run markdownlint on docs for every pull request"
# після StarUML (крок 3) і вставки PNG у .docx (крок 4):
git add docs/uml/kurin-use-cases.mdj docs/uml/kurin-use-cases.png docs/F2_Diploma_Chapters1-2_Дармограй.docx
git commit -m "docs: add use case diagram from StarUML"
npx markdownlint-cli2@0.23.2    # те саме, що в CI; має бути "0 issues" (потрібен Node 22+)
git push -u origin feature/lab7-docs-quality-gate
```

Далі на GitHub: **Compare & pull request** → base `master` ← compare `feature/lab7-docs-quality-gate` → заголовок
`docs: consolidate chapter 2, specify use cases and API topology, add docs quality gate` → опис – вміст
`pr_body.md` → **Create pull request**. **Не зливати** до перевірки викладачем.

Якщо PR Розділу 2 (`feature/chapter2-architecture`) ще не злитий, у цьому PR будуть видні і його коміти – це
нормально (так само було з ЛР5–6). Якщо хочете чистий diff – відкрийте PR з base `feature/chapter2-architecture`.

**Якщо пайплайн червоний:** відкрийте Checks → docs-linter → крок «Lint Markdown» – там `файл:рядок правило`.
Найімовірніше – щось, дописане вручну (наприклад, рядок з Google-посиланням довший за 120 символів – перенесіть).
Виправте, `git commit`, `git push` – перевірка перезапуститься сама.

## 5. Ризики, про які варто знати

- Логи AI Insights складено в сесії з Claude Code (це зазначено в дисклеймері, як і в ЛР5–6). Чернетки v0 – це
  навмисно слабкі первинні версії для краш-тесту. Чесніше і безпечніше для оцінки – прогнати промпти самому
  (крок 3.2) і за потреби доповнити логи.
- Штраф −1 за транслітерацію: у текстах використано лише оригінальні терміни (Use Case, Happy Path, Pull Request,
  Markdown). Перевірте свої дописки на «юскейс», «свагер», «маркдаун».
