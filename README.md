# Sengencho Rent Watch

浅間町向けの賃貸ウォッチャーです。`1時間おき` の収集を前提に、物件の新着・更新・経過日数を一覧表示します。

## できること

- 複数の賃貸サイトを定期収集
- 物件情報を正規化して `SQLite` に保存
- 初回掲載日、最終確認日、更新基準日の差分管理
- `何日前に登録 / 更新されたか` の表示
- 浅間町向けの住所・駅キーワードで絞り込み
- Web UI 上での検索、賃料、面積、徒歩、掲載日数での絞り込み
- 賃料順、広さ順、駅近順での並び替え
- 緯度経度を付与した物件の地図表示
- 重複物件の統合表示
- `新着 / 更新 / 再掲載` の状態表示
- ブラウザ内の `お気に入り / 除外`
- 保存済み通知条件のCLIチェック

現在の実装済み取得元:

- `SUUMO`
- `CHINTAI`
- `Yahoo不動産`

補足:

- `CHINTAI` はページ送り対応済み
- `HOME'S` は取得時点で `403`
- `アパマンショップ` は公開HTMLが店舗一覧で、物件一覧の静的取得には未対応

## 前提

- Python `3.11+`
- ネットワーク接続環境
- 各対象サイトの利用規約と `robots.txt` を事前確認すること

## セットアップ

```bash
cd /Users/kmr/Desktop/asamacho-rent-watch
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 設定

アプリ全体の設定は [config/app.toml](/Users/kmr/Desktop/asamacho-rent-watch/config/app.toml)、
サイトごとの収集設定は [config/sources.toml](/Users/kmr/Desktop/asamacho-rent-watch/config/sources.toml) を編集します。

初期状態では、浅間町を拾いやすいように以下のキーワードが入っています。

- `横浜市西区浅間町`

## 実行

データ収集:

```bash
python3 -m app.cli collect
```

初回の実データ投入後は、続けて座標を付与します。

```bash
python3 -m app.cli geocode --limit 300
```

通知候補の確認:

```bash
python3 -m app.cli notify
```

デモデータ投入:

```bash
python3 -m app.cli seed-demo
```

座標の付与:

```bash
python3 -m app.cli geocode --limit 100
```

Web UI 起動:

```bash
python3 -m app.cli serve --port 8000
```

起動後に `http://127.0.0.1:8000` を開くと、一覧と地図が同時に見られます。

検索例:

```text
http://127.0.0.1:8000?max_walk=10&max_rent_man=13&sort=walk
```

地図を埋める流れ:

1. `collect` で物件を保存
2. `geocode` で住所に座標を付与
3. `serve` で地図つき画面を開く

ブラウザ側の補助機能:

- `お気に入りのみ`
- `除外を隠す`
- カードごとの `お気に入り / 除外`
- 別タブから戻った時のスクロール位置復元

## 1時間おきの実行例

`cron` で毎時 `5分` に収集し、その後に座標付与する例です。

```cron
5 * * * * cd /Users/kmr/Desktop/asamacho-rent-watch && /Users/kmr/Desktop/asamacho-rent-watch/.venv/bin/python -m app.cli collect >> /tmp/asamacho-rent-watch.log 2>&1
12 * * * * cd /Users/kmr/Desktop/asamacho-rent-watch && /Users/kmr/Desktop/asamacho-rent-watch/.venv/bin/python -m app.cli geocode --limit 80 >> /tmp/asamacho-rent-watch.log 2>&1
```

## 現状の制約

- サイトごとの `selector` は実サイトに合わせて検証が必要です
- 更新日を持たないサイトは、取得日時を更新基準日に使います
- `HOME'S` は静的取得時に `403` で未接続です
- `アパマンショップ` は物件一覧ページの静的入口が未接続です

## 構成

- [app/cli.py](/Users/kmr/Desktop/asamacho-rent-watch/app/cli.py): CLI エントリポイント
- [app/collector.py](/Users/kmr/Desktop/asamacho-rent-watch/app/collector.py): 収集と差分保存
- [app/storage.py](/Users/kmr/Desktop/asamacho-rent-watch/app/storage.py): SQLite 永続化
- [app/web.py](/Users/kmr/Desktop/asamacho-rent-watch/app/web.py): 一覧表示サーバ
- [app/geocoder.py](/Users/kmr/Desktop/asamacho-rent-watch/app/geocoder.py): 住所から座標を付与
- [config/alerts.toml](/Users/kmr/Desktop/asamacho-rent-watch/config/alerts.toml): 通知条件
- [config/sources.toml](/Users/kmr/Desktop/asamacho-rent-watch/config/sources.toml): サイト別設定
