# `earthindex_pull`: Fetch Features from Earth Index API

[Earth Index](https://earthindex.ai) is an AI-powered search tool developed by [Earth Genome](https://earthgenome.org) to identify environmental features (for example, illegal mining, deforestation, solar farms) using satellite imagery and machine learning. It acts as a search engine for the planet, allowing users to find specific objects by analyzing genetic signatures or embeddings from satellite data. 

This script fetches detected features in a given project from the Earth Index API. We also store the project metadata as a JSON file on the datalake.

### Parameters required

- **Project ID**: You can get the Project ID from the URL of the project page in Earth Index: `https://app.earthindex.ai/project-view/<project_id>/...`. 
- **API Key**: You can generate an API Key in Manage Account -> API Keys.

## Notes

- Currently, we don't differentiate between the different annotation types ("tags", e.g. predicted, positive, negative). We download and store the entire dataset as returned by the API.
- As we are evolving our understanding of how to integrate with Earth Index and their API is under active development, we are not yet using a Windmill Resource to encapsulate the API key or Project ID. Instead, we are passing them as script parameters directly.
- Currently, the script assumes that there is exactly 1 layer per project. As future features like Deep Search and change detection are added to Earth Index, multiple layers may be returned — at which point this script should be adapted to handle multiple layers.

## 📚 Reference

* API Guide: https://api.earthindex.dev/docs/index.html