import psycopg

from f.connectors.localcontexts.localcontexts_pull import (
    main,
    transform_labels_for_db,
)
from f.connectors.localcontexts.tests.assets import server_responses


def test_transform_labels_for_db():
    """Test the label transformation function with translations as columns."""
    project_data = server_responses.SAMPLE_PROJECT
    normalized_title = "guardian_connector_lc_labels"

    result = transform_labels_for_db(project_data, normalized_title)

    # Should have 2 BC labels + 3 TK labels = 5 total
    assert len(result) == 5

    # Check BC label without translations
    bc_label = next(
        r for r in result if r["_id"] == "bc000000-0000-0000-0000-000000000001"
    )
    assert bc_label["project_title"] == normalized_title
    assert bc_label["label_category"] == "BC"
    assert bc_label["name"] == "BC Provenance (BC P)"
    assert bc_label["label_type"] == "provenance"
    assert bc_label["language_tag"] == "en"
    assert bc_label["language"] == "English"
    assert "inherent interest" in bc_label["label_text"]
    assert bc_label["community_id"] == 118
    assert bc_label["community_name"] == "My Community"
    assert bc_label["data_source"] == "Local Contexts"
    assert (
        bc_label["img_url"]
        == "https://localcontexts.org/wp-content/uploads/2025/04/bc-provenance.png"
    )
    # No translation columns for this label
    assert "name_way" not in bc_label
    assert "label_text_pt" not in bc_label

    # Check TK label with translations (2 translations: Wayana and Swahili)
    tk_label = next(
        r for r in result if r["_id"] == "tk000000-0000-0000-0000-000000000001"
    )
    assert tk_label["label_category"] == "TK"
    assert tk_label["name"] == "TK Kultural Sensitif (TK KS)"
    assert tk_label["label_type"] == "culturally_sensitive"
    assert tk_label["language_tag"] == "srn"
    assert tk_label["language"] == "Sranan Tongo"
    # Check translation columns exist
    assert "name_way" in tk_label
    assert tk_label["name_way"] == "TK Kulutuwano Sensitipu (TK KS)"
    assert "label_text_way" in tk_label
    assert "Nono etikëtï" in tk_label["label_text_way"]
    assert "language_way" in tk_label
    assert tk_label["language_way"] == "Wayana"
    # Check Swahili translation
    assert "name_sw" in tk_label
    assert tk_label["name_sw"] == "TK Nyenzo Nyeti Kitamaduni (TK NK)"
    assert "label_text_sw" in tk_label
    assert "Lebo hii" in tk_label["label_text_sw"]
    assert "language_sw" in tk_label
    assert tk_label["language_sw"] == "Swahili (macrolanguage)"

    # Check another TK label with Portuguese translation
    tk_label_cv = next(
        r for r in result if r["_id"] == "tk000000-0000-0000-0000-000000000002"
    )
    assert tk_label_cv["name"] == "TK Community Voice (TK CV)"
    assert "name_pt" in tk_label_cv
    assert tk_label_cv["name_pt"] == "Voz da Comunidade TK (TK CV)"
    assert "label_text_pt" in tk_label_cv
    assert "Este selo" in tk_label_cv["label_text_pt"]

    # Check BC label with audiofile
    bc_with_audio = next(
        r for r in result if r["_id"] == "bc000000-0000-0000-0000-000000000002"
    )
    assert bc_with_audio["name"] == "BC Consent Verified (BC CV)"
    assert bc_with_audio["audiofile"] is not None
    assert "storage.googleapis.com" in bc_with_audio["audiofile"]


def test_script_e2e(localcontexts_server, pg_database, tmp_path):
    """Test the full script end-to-end."""
    asset_storage = tmp_path / "datalake"

    main(
        localcontexts_server.localcontexts_project,
        pg_database,
        asset_storage,
    )

    # Check that project.json was saved
    project_json_path = (
        asset_storage
        / "localcontexts"
        / "guardian_connector_lc_labels"
        / "project.json"
    )
    assert project_json_path.exists()

    # Check that label images were downloaded
    labels_dir = (
        asset_storage / "localcontexts" / "guardian_connector_lc_labels" / "labels"
    )
    assert (labels_dir / "bc-provenance.png").exists()
    assert (labels_dir / "bc-consent-verified.png").exists()
    assert (labels_dir / "tk-culturally-sensitive.png").exists()
    assert (labels_dir / "tk-community-voice.png").exists()
    assert (labels_dir / "tk-attribution.png").exists()

    # Check that audio file was downloaded
    assert (labels_dir / "FAKE_AUDIO_ID.mp3").exists()

    # Check database table was created and populated
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            # Check labels table
            cursor.execute(
                "SELECT COUNT(*) FROM localcontexts_guardian_connector_lc_labels"
            )
            assert cursor.fetchone()[0] == 5

            # Check BC label data
            cursor.execute(
                "SELECT name, label_category, label_type, language, community_name FROM localcontexts_guardian_connector_lc_labels WHERE _id = 'bc000000-0000-0000-0000-000000000001'"
            )
            row = cursor.fetchone()
            assert row[0] == "BC Provenance (BC P)"
            assert row[1] == "BC"
            assert row[2] == "provenance"
            assert row[3] == "English"
            assert row[4] == "My Community"

            # Check TK label data with translations as columns
            cursor.execute(
                "SELECT name, label_category, label_type, language, name_way, label_text_way, language_way, name_sw, language_sw FROM localcontexts_guardian_connector_lc_labels WHERE _id = 'tk000000-0000-0000-0000-000000000001'"
            )
            row = cursor.fetchone()
            assert row[0] == "TK Kultural Sensitif (TK KS)"
            assert row[1] == "TK"
            assert row[2] == "culturally_sensitive"
            assert row[3] == "Sranan Tongo"
            # Check Wayana translation columns
            assert row[4] == "TK Kulutuwano Sensitipu (TK KS)"
            assert "Nono etikëtï" in row[5]
            assert row[6] == "Wayana"
            # Check Swahili translation columns
            assert row[7] == "TK Nyenzo Nyeti Kitamaduni (TK NK)"
            assert row[8] == "Swahili (macrolanguage)"

            # Check Portuguese translation column
            cursor.execute(
                "SELECT name_pt, label_text_pt FROM localcontexts_guardian_connector_lc_labels WHERE _id = 'tk000000-0000-0000-0000-000000000002'"
            )
            row = cursor.fetchone()
            assert row[0] == "Voz da Comunidade TK (TK CV)"
            assert "Este selo" in row[1]

            # Check that audiofile is stored
            cursor.execute(
                "SELECT audiofile FROM localcontexts_guardian_connector_lc_labels WHERE _id = 'bc000000-0000-0000-0000-000000000002'"
            )
            audiofile_url = cursor.fetchone()[0]
            assert audiofile_url is not None
            assert "storage.googleapis.com" in audiofile_url

            # Check data_source field
            cursor.execute(
                "SELECT DISTINCT data_source FROM localcontexts_guardian_connector_lc_labels"
            )
            assert cursor.fetchone()[0] == "Local Contexts"


def test_project_with_no_labels(mocked_responses, pg_database, tmp_path):
    """Test handling of a project with no labels."""
    asset_storage = tmp_path / "datalake"

    # Mock a project with no labels
    api_key = "test-api-key"
    project_id = "empty-project-id"
    server_url = "https://sandbox.localcontextshub.org"

    empty_project = {
        "unique_id": project_id,
        "title": "Empty Project",
        "bc_labels": [],
        "tk_labels": [],
    }

    mocked_responses.get(
        f"{server_url}/api/v2/projects/{project_id}",
        json=empty_project,
        status=200,
    )

    localcontexts_project_dict = dict(
        server_url=server_url,
        api_key=api_key,
        project_id=project_id,
    )

    # Should complete without error
    main(localcontexts_project_dict, pg_database, asset_storage)

    # Project JSON should still be saved
    project_json_path = (
        asset_storage / "localcontexts" / "empty_project" / "project.json"
    )
    assert project_json_path.exists()

    # No database tables should be created
    with psycopg.connect(autocommit=True, **pg_database) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_name = 'localcontexts_empty_project'
                )
            """
            )
            assert not cursor.fetchone()[0]


def test_skipped_attachments(localcontexts_server, pg_database, tmp_path):
    """Test that existing attachments are skipped on re-run."""
    asset_storage = tmp_path / "datalake"

    # Run once
    main(localcontexts_server.localcontexts_project, pg_database, asset_storage)

    labels_dir = (
        asset_storage / "localcontexts" / "guardian_connector_lc_labels" / "labels"
    )
    bc_provenance_path = labels_dir / "bc-provenance.png"
    assert bc_provenance_path.exists()

    # Modify file content to verify it's not overwritten
    original_content = bc_provenance_path.read_bytes()
    bc_provenance_path.write_bytes(b"modified content")

    # Run again
    main(localcontexts_server.localcontexts_project, pg_database, asset_storage)

    # File should not be overwritten
    modified_content = bc_provenance_path.read_bytes()
    assert modified_content == b"modified content"
    assert original_content != modified_content
