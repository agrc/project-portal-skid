from unittest.mock import MagicMock

import geopandas as gpd
import pytest
import requests
from shapely.geometry import Point

from project_portal_skid import main


def test_get_secrets_from_gcp_location(mocker):
    mocker.patch("pathlib.Path.exists", return_value=True)
    mocker.patch("pathlib.Path.read_text", return_value='{"foo":"bar"}')

    secrets = main._get_secrets()

    assert secrets == {"foo": "bar"}


def test_get_secrets_from_local_location(mocker):
    exists_mock = mocker.Mock(side_effect=[False, True])
    mocker.patch("pathlib.Path.exists", new=exists_mock)
    mocker.patch("pathlib.Path.read_text", return_value='{"foo":"bar"}')

    secrets = main._get_secrets()

    assert secrets == {"foo": "bar"}
    assert exists_mock.call_count == 2


# ── helpers ───────────────────────────────────────────────────────────────────


def _make_response(json_data: dict, status_code: int = 200) -> MagicMock:
    """Helper to build a mock requests.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)

    return resp


# ── _fetch_projects ────────────────────────────────────────────────────────────


class TestFetchProjects:
    def test_fetch_projects_single_page_returns_projects_list(self, mocker):
        """Single page response with a 'projects' key returns all records."""
        projects = [{"id": "1"}, {"id": "2"}]
        mock_get = mocker.patch(
            "requests.Session.get",
            return_value=_make_response({"projects": projects}),
        )

        result = main._fetch_projects(api_key="test-key")

        assert result == projects
        assert mock_get.call_count == 1

    def test_fetch_projects_uses_results_key_as_fallback(self, mocker):
        """Response with a 'results' key (no 'projects') is handled correctly."""
        projects = [{"id": "3"}]
        mocker.patch(
            "requests.Session.get",
            return_value=_make_response({"results": projects}),
        )

        result = main._fetch_projects(api_key="test-key")

        assert result == projects

    def test_fetch_projects_paginates_until_no_next_search_after(self, mocker):
        """Cursor-based pagination fetches all pages and concatenates results."""
        page1 = _make_response({"projects": [{"id": "1"}], "nextSearchAfter": "cursor-abc"})
        page2 = _make_response({"projects": [{"id": "2"}]})
        mocker.patch("requests.Session.get", side_effect=[page1, page2])

        result = main._fetch_projects(api_key="test-key")

        assert result == [{"id": "1"}, {"id": "2"}]

    def test_fetch_projects_page_size_none_defaults_to_10000(self, mocker):
        """page_size=None is coerced to 10000."""
        mock_get = mocker.patch(
            "requests.Session.get",
            return_value=_make_response({"projects": []}),
        )

        main._fetch_projects(api_key="test-key", page_size=None)

        _, kwargs = mock_get.call_args
        assert kwargs["params"]["pageSize"] == 10000

    def test_fetch_projects_page_size_zero_defaults_to_10000(self, mocker):
        """page_size=0 (invalid) is coerced to 10000."""
        mock_get = mocker.patch(
            "requests.Session.get",
            return_value=_make_response({"projects": []}),
        )

        main._fetch_projects(api_key="test-key", page_size=0)

        _, kwargs = mock_get.call_args
        assert kwargs["params"]["pageSize"] == 10000

    def test_fetch_projects_page_size_capped_at_10000(self, mocker):
        """page_size values above 10000 are clamped to 10000."""
        mock_get = mocker.patch(
            "requests.Session.get",
            return_value=_make_response({"projects": []}),
        )

        main._fetch_projects(api_key="test-key", page_size=99999)

        _, kwargs = mock_get.call_args
        assert kwargs["params"]["pageSize"] == 10000

    def test_fetch_projects_retries_on_429(self, mocker):
        """A 429 response causes a sleep-and-retry before succeeding."""
        rate_limited = _make_response({}, status_code=429)
        success = _make_response({"projects": [{"id": "1"}]})
        mocker.patch("requests.Session.get", side_effect=[rate_limited, success])
        mocker.patch("time.sleep")

        result = main._fetch_projects(api_key="test-key")

        assert result == [{"id": "1"}]

    def test_fetch_projects_reraises_request_exception(self, mocker):
        """A requests.RequestException propagates to the caller."""
        mocker.patch(
            "requests.Session.get",
            side_effect=requests.ConnectionError("network error"),
        )

        with pytest.raises(requests.ConnectionError):
            main._fetch_projects(api_key="test-key")

    def test_fetch_projects_raises_on_http_error(self, mocker):
        """A non-2xx, non-429 response raises an HTTPError."""
        mocker.patch(
            "requests.Session.get",
            return_value=_make_response({}, status_code=500),
        )

        with pytest.raises(requests.HTTPError):
            main._fetch_projects(api_key="test-key")

    def test_fetch_projects_raises_on_non_list_projects(self, mocker):
        """A response where 'projects' is not a list raises ValueError."""
        mocker.patch(
            "requests.Session.get",
            return_value=_make_response({"projects": "unexpected-string"}),
        )

        with pytest.raises(ValueError, match="not a list"):
            main._fetch_projects(api_key="test-key")

    def test_fetch_projects_empty_page_stops_pagination(self, mocker):
        """A response with an empty projects list and no cursor stops iteration."""
        mocker.patch(
            "requests.Session.get",
            return_value=_make_response({"projects": []}),
        )

        result = main._fetch_projects(api_key="test-key")

        assert result == []


# ── _make_point ────────────────────────────────────────────────────────────────


class TestMakePoint:
    def test_make_point_returns_point_for_valid_lat_lon(self):
        """A record with valid lat/lon produces the correct Point(lon, lat)."""
        row = {"locationGeoPoint": {"lat": 40.5, "lon": -111.9}}

        result = main._make_point(row)

        assert isinstance(result, Point)
        assert result.x == pytest.approx(-111.9)
        assert result.y == pytest.approx(40.5)

    def test_make_point_returns_none_when_location_key_missing(self):
        """A record without 'locationGeoPoint' returns None."""
        result = main._make_point({"id": "1"})

        assert result is None

    def test_make_point_returns_none_when_location_is_none(self):
        """A record where 'locationGeoPoint' is None returns None."""
        result = main._make_point({"locationGeoPoint": None})

        assert result is None

    def test_make_point_returns_none_when_location_is_not_dict(self):
        """A record where 'locationGeoPoint' is a non-dict value returns None."""
        result = main._make_point({"locationGeoPoint": "40.5,-111.9"})

        assert result is None

    def test_make_point_returns_none_when_lat_is_missing(self):
        """A locationGeoPoint dict without 'lat' returns None."""
        result = main._make_point({"locationGeoPoint": {"lon": -111.9}})

        assert result is None

    def test_make_point_returns_none_when_lon_is_missing(self):
        """A locationGeoPoint dict without 'lon' returns None."""
        result = main._make_point({"locationGeoPoint": {"lat": 40.5}})

        assert result is None

    def test_make_point_returns_none_when_row_is_not_dict(self):
        """A non-dict row (e.g., None) returns None without raising."""
        result = main._make_point(None)

        assert result is None


# ── _projects_to_gdf ──────────────────────────────────────────────────────────


class TestProjectsToGdf:
    def test_projects_to_gdf_returns_geodataframe_for_non_empty_list(self):
        """A non-empty list of project dicts produces a GeoDataFrame."""
        projects = [{"id": "1", "locationGeoPoint": {"lat": 40.5, "lon": -111.9}}]

        result = main._projects_to_gdf(projects)

        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 1

    def test_projects_to_gdf_sets_crs_to_epsg_4326(self):
        """The resulting GeoDataFrame uses the EPSG:4326 CRS."""
        projects = [{"id": "1", "locationGeoPoint": {"lat": 40.5, "lon": -111.9}}]

        result = main._projects_to_gdf(projects)

        assert result.crs.to_epsg() == 4326

    def test_projects_to_gdf_geometry_is_correct_point(self):
        """Each valid project record produces the expected Point geometry."""
        projects = [{"id": "1", "locationGeoPoint": {"lat": 40.5, "lon": -111.9}}]

        result = main._projects_to_gdf(projects)

        geom = result.iloc[0].geometry
        assert isinstance(geom, Point)
        assert geom.x == pytest.approx(-111.9)
        assert geom.y == pytest.approx(40.5)

    def test_projects_to_gdf_missing_location_yields_null_geometry(self):
        """A project record without a valid location has a null geometry entry."""
        projects = [{"id": "1"}]

        result = main._projects_to_gdf(projects)

        assert result.iloc[0].geometry is None

    def test_projects_to_gdf_empty_list_returns_empty_geodataframe(self):
        """An empty input list produces an empty GeoDataFrame."""
        result = main._projects_to_gdf([])

        assert isinstance(result, gpd.GeoDataFrame)
        assert len(result) == 0

    def test_projects_to_gdf_mixed_valid_and_invalid_locations(self):
        """Records with and without valid locations are handled in the same GeoDataFrame."""
        projects = [
            {"id": "1", "locationGeoPoint": {"lat": 40.5, "lon": -111.9}},
            {"id": "2"},
            {"id": "3", "locationGeoPoint": {"lat": 41.0, "lon": -112.0}},
        ]

        result = main._projects_to_gdf(projects)

        assert len(result) == 3
        assert isinstance(result.iloc[0].geometry, Point)
        assert result.iloc[1].geometry is None
        assert isinstance(result.iloc[2].geometry, Point)


# ── _replace_null_geometries ───────────────────────────────────────────────────


class TestReplaceNullGeometries:
    def _make_gdf(self, geometries):
        """Create a GeoDataFrame from a list of geometries (Point or None)."""

        return gpd.GeoDataFrame({"id": range(len(geometries))}, geometry=geometries, crs="EPSG:4326")

    def test_replace_null_geometries_returns_zero_count_when_no_nulls(self):
        """A GeoDataFrame with no null geometries returns the original data and count 0."""
        gdf = self._make_gdf([Point(1, 2), Point(3, 4)])

        result_gdf, null_count = main._replace_null_geometries(gdf)

        assert null_count == 0
        assert len(result_gdf) == 2
        assert result_gdf.iloc[0].geometry == Point(1, 2)
        assert result_gdf.iloc[1].geometry == Point(3, 4)

    def test_replace_null_geometries_replaces_single_null_with_point_zero(self):
        """A single null geometry is replaced with Point(0, 0) and count is 1."""
        gdf = self._make_gdf([None])

        result_gdf, null_count = main._replace_null_geometries(gdf)

        assert null_count == 1
        assert result_gdf.iloc[0].geometry == Point(0, 0)

    def test_replace_null_geometries_replaces_all_nulls_and_returns_correct_count(self):
        """All null geometries are replaced with Point(0, 0) and count matches the number of nulls."""
        gdf = self._make_gdf([None, None, None])

        result_gdf, null_count = main._replace_null_geometries(gdf)

        assert null_count == 3
        assert all(result_gdf.geometry == Point(0, 0))

    def test_replace_null_geometries_only_replaces_null_rows_in_mixed_gdf(self):
        """Only null geometry rows are replaced; valid geometries are preserved."""
        gdf = self._make_gdf([Point(1, 2), None, Point(3, 4)])

        result_gdf, null_count = main._replace_null_geometries(gdf)

        assert null_count == 1
        assert result_gdf.iloc[0].geometry == Point(1, 2)
        assert result_gdf.iloc[1].geometry == Point(0, 0)
        assert result_gdf.iloc[2].geometry == Point(3, 4)

    def test_replace_null_geometries_does_not_mutate_original_gdf(self):
        """The original GeoDataFrame is not modified when null geometries are replaced."""
        gdf = self._make_gdf([None, Point(1, 2)])
        original_geom = gdf.iloc[0].geometry

        main._replace_null_geometries(gdf)

        assert gdf.iloc[0].geometry is original_geom

    def test_replace_null_geometries_returns_geodataframe_type(self):
        """The returned object is always a GeoDataFrame."""
        gdf = self._make_gdf([Point(1, 2)])

        result_gdf, _ = main._replace_null_geometries(gdf)

        assert isinstance(result_gdf, gpd.GeoDataFrame)
