# Order-to-fulfilment glossary (TPC-H test domain)

This page explains the words the sales and fulfilment teams use for the
TPC-H order data. It is test content for OntoBricks Evolve.

## Terms

| Term we use | What it means in the data |
|---|---|
| VOR (vendor of record) | A **supplier**. The VOR on a line is the supplier that fulfilled that line item. |
| Account | A **customer**. Account names look like `Customer#000001510`. |
| Market | The **nation** (country) a customer is based in. Sales says "the GERMANY market". It never means the market segment. |
| Segment | The customer's **market segment** (`AUTOMOBILE`, `BUILDING`, `FURNITURE`, `HOUSEHOLD`, `MACHINERY`). |
| Theatre | A **region**: AFRICA, AMERICA, ASIA, EUROPE or MIDDLE EAST. |
| SKU | A **part**. A SKU is named by its part name, for example `goldenrod lavender spring chocolate lace`. |
| Shipment line | A **line item** of an order. |
| Book | An **order** placed by an account. |

## How the things relate

- Each shipment line belongs to exactly one order.
- Each shipment line records the VOR that fulfilled it and the SKU it shipped.
- "Orders from VOR X" means the orders that contain at least one shipment
  line fulfilled by X. It does not mean orders whose account is in the same
  market as X. Two things being in the same nation is not a business link.
- The VOR price list (which VORs are able to supply which SKUs) is a
  catalogue. It does not say what was shipped. Use the shipment lines for
  anything about real orders.
- Each account is based in one market, and each market belongs to one theatre.

## Questions people ask

- Which books did an account place?
- Which accounts are in a given market?
- Which VORs operate in a theatre?
- Which books contain shipment lines fulfilled by a VOR?
- Which books contain a given SKU?
