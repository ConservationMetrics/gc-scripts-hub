# eBird

[**eBird**](https://ebird.org/) is the Cornell Lab of Ornithology's database of bird observations. `ebird_pull.py` downloads an observation list from the [eBird API 2.0](https://documenter.getpostman.com/view/664302/S1ENwy59) and writes it to the datalake and PostgreSQL.

## `ebird_pull.py`

The **Observations** dropdown selects one endpoint:

1. **Recent observations in a region** — `GET /v2/data/obs/{regionCode}/recent`
2. **Recent notable observations in a region** — `GET /v2/data/obs/{regionCode}/recent/notable?detail=full`
3. **Recent nearby observations** — `GET /v2/data/obs/geo/recent?lat=&lng=`
4. **Recent nearby notable observations** — `GET /v2/data/obs/geo/recent/notable?lat=&lng=&detail=full`
5. **Historic observations on a date** — `GET /v2/data/obs/{regionCode}/historic/{year}/{month}/{day}`

Each run is a single request. Region and nearby results are the most recent observation of each species. Notable observations are locally or nationally rare, or otherwise unusual, and can include more than one row for the same species. Notable requests send `detail=full`, which adds observer and place-name fields.

eBird caps these endpoints at 10,000 observations per request.

The API token is sent as the `X-eBirdApiToken` header. **API token** is a Windmill variable: create the key once as a secret variable, then select that variable on the run or schedule. Request a key at [ebird.org/api/keygen](https://ebird.org/api/keygen).

> [!NOTE] Searching by geography is not available
> Unlike the [GBIF](../gbif/README.md) and [iNaturalist](../inaturalist/README.md) connectors, eBird cannot be searched by a specific geography such as a bounding box. A subnational region must be used.

> [!NOTE] Batch-downloading all historical observations is not available
> It is not possible to batch-download all historical observations. **Historic observations on a date** returns one day at a time: enter that day as `YYYY-MM-DD` with a region code. Recent observation lists cover at most the last 30 days, and those requests use the API default of 14 days.

> [!TIP]
> Use the eBird [Region Info](https://documenter.getpostman.com/view/664302/S1ENwy59#07c64240-6359-4688-9c4f-ff3d678a7248) endpoint to look up a subnational code. For Suriname (`SR`):
>
> ```bash
> curl --location 'https://api.ebird.org/v2/ref/region/list/subnational1/SR' \
>   --header 'X-eBirdApiToken: YOUR_API_TOKEN'
> ```
>
> Alternatively, you can reference [Statoids](https://statoids.com/), which also lists subnational region codes. Replace the dot with a hyphen. Sipaliwini is `SR.SI` on Statoids and `SR-SI` in eBird.

## Terms of use

Review the [eBird API Terms of Use](https://www.birds.cornell.edu/home/ebird-api-terms-of-use/) before storing or sharing observations. Attribute the records to eBird.

## 📚 Reference

- [eBird API 2.0](https://documenter.getpostman.com/view/664302/S1ENwy59)
- [Request an eBird API key](https://ebird.org/api/keygen)
- [eBird API Terms of Use](https://www.birds.cornell.edu/home/ebird-api-terms-of-use/)
- [Region Info](https://documenter.getpostman.com/view/664302/S1ENwy59#07c64240-6359-4688-9c4f-ff3d678a7248)
- [Historic observations on a date](https://documenter.getpostman.com/view/664302/S1ENwy59#2d8c6ee8-c435-4e91-9f66-6d3eeb09edd2)
