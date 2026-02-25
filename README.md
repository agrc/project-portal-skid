# UGRC Utah Project Portal Skid

[![Push Events](https://github.com/agrc/project-portal-skid/actions/workflows/push.yml/badge.svg)](https://github.com/agrc/project-portal-skid/actions/workflows/push.yml)

This skid extracts data from the [Utah Project Portal](https://upp.utah.gov/) and loads them into an AGOL hosted feature service for use in AGOL-based products.

## Process

The skid uses the [Utah Project Portal API](https://api.upp.utah.gov) and an API key generated on the homepage to get all the projects available on the Portal and load them into a GeoDataFrame. The lat/long coordinates from the source data are converted into WGS84 points.

The project data are then cleaned, which includes creating points on Null Island (0, 0) for any projects lacking lat/long data.

Finally, palletjack loads them into the AGOL hosted feature service.

## Environment

This skid is run every Monday morning at 3:00 AM in GCP as a Cloud Run job triggered by Cloud Scheduler.

## Attribution

This project was developed with the assistance of [GitHub Copilot](https://github.com/features/copilot).
