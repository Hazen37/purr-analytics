# Marketplace Product Mapping Template

Файл `marketplace_product_mapping.template.csv` уже заполнен реальными `canonical_products` из базы.

Что заполнять:
- `yandex_market` + `offer_id`: сюда вноси `offerId` или `shopSku` из Яндекс Маркета.
- `wildberries` + `nm_id`: сюда вноси `nmID` из Wildberries.

Правила:
- Одна строка = одна точная привязка внешнего товара к каноническому товару.
- `canonical_key`, `canonical_name`, `flavor`, `grams` уже предзаполнены и должны оставаться эталоном.
- `external_key` нужно заполнить вручную.
- Если для WB удобнее маппить по `article`, можно дублировать строку и заменить `external_key_type` на `article`.
- Пустые строки импортировать нельзя: сначала заполни `external_key`.

Импорт:

```bash
python -m src.cli.import_marketplace_product_mapping src/cli/marketplace_product_mapping.template.csv
```
