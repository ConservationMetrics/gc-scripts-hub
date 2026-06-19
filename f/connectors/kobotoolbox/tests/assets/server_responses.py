import posixpath
from urllib.parse import urlencode

nested_form_id = "fixture_nested_household"


def kobo_form(uri, form_id, form_name):
    return {
        "name": form_name,
        "uid": form_id,
        "owner__username": "kobo_admin",
        "data": posixpath.join(uri, "api/v2/assets", form_id, "data/"),
        "content": {
            "schema": "1",
            "survey": [
                {
                    "name": "Record_your_current_location",
                    "type": "geopoint",
                    "label": [
                        "Record your current location",
                        "Registre la ubicación actual",
                        "Registre a localização atual",
                    ],
                    "$qpath": "Record_your_current_location",
                    "$xpath": "Record_your_current_location",
                    "$autoname": "Record_your_current_location",
                },
                {
                    "name": "Estimate_height_of_your_tree_in_meters",
                    "type": "integer",
                    "label": [
                        "Estimate the height of your tree (in meters)",
                        "Estime la altura de su árbol (en metros)",
                        "Estime a altura da sua árvore (em metros)",
                    ],
                    "$qpath": "Estimate_height_of_your_tree_in_meters",
                    "$xpath": "Estimate_height_of_your_tree_in_meters",
                    "$autoname": "Estimate_height_of_your_tree_in_meters",
                },
                {
                    "name": "I_like_this_tree_because",
                    "type": "select_multiple",
                    "label": [
                        "Why do you like this tree?",
                        "¿Por qué te gusta este árbol?",
                        "Por que você gosta desta árvore?",
                    ],
                    "select_from_list_name": "tree_reasons",
                    "$qpath": "I_like_this_tree_because",
                    "$xpath": "I_like_this_tree_because",
                    "$autoname": "I_like_this_tree_because",
                },
                {
                    "name": "Take_a_photo_of_this_tree",
                    "type": "image",
                    "label": [
                        "Take a photo of this tree",
                        "Toma una foto de este árbol",
                        "Tire uma foto desta árvore",
                    ],
                    "$qpath": "Take_a_photo_of_this_tree",
                    "$xpath": "Take_a_photo_of_this_tree",
                    "$autoname": "Take_a_photo_of_this_tree",
                },
            ],
            "choices": [
                {
                    "list_name": "tree_reasons",
                    "name": "shade",
                    "label": ["Shade", "Sombra", "Sombra"],
                },
                {
                    "list_name": "tree_reasons",
                    "name": "food",
                    "label": ["Food", "Comida", "Comida"],
                },
                {
                    "list_name": "tree_reasons",
                    "name": "wildlife_habitat",
                    "label": [
                        "Wildlife Habitat",
                        "Hábitat de vida silvestre",
                        "Habitat da vida selvagem",
                    ],
                },
                {
                    "list_name": "tree_reasons",
                    "name": "beauty",
                    "label": ["Beauty", "Belleza", "Beleza"],
                },
            ],
            "settings": {"version": "1", "default_language": "English (en)"},
            "translated": ["label"],
            "translations": ["English (en)", "Spanish (es)", "Portuguese (pt)"],
        },
    }


def _get_all_submissions(uri, form_id):
    """Return all submission records for testing."""
    return [
            {
                "_id": 124961136,
                "formhub/uuid": "f7bef041e8624f09946bff05ee5cbd4b",
                "start": "2021-11-16T15:44:56.464-08:00",
                "end": "2021-11-16T15:46:17.524-08:00",
                "today": "2021-11-16",
                "username": "cmi_admin_kobo_test",
                "deviceid": "collect:pEo4VhQlQDnvDCNj",
                "Record_your_current_location": "36.97012 -122.0109429 -14.432169171089349 4.969",
                "Estimate_height_of_your_tree_in_meters": "18",
                "I_like_this_tree_because": "shade food wildlife_habitat beauty",
                "Take_a_photo_of_this_tree": "1637106370994.jpg",
                "__version__": "vLnrnT6Pvzgeh4DVfj6eDz",
                "meta/instanceID": "uuid:e58da38d-3eee-4bd7-8512-4a97ea8fbb01",
                "_xform_id_string": form_id,
                "_uuid": "e58da38d-3eee-4bd7-8512-4a97ea8fbb01",
                "_attachments": [
                    {
                        "download_url": f"{uri}/api/v2/assets/{form_id}/data/124961136/attachments/46493730/",
                        "download_large_url": f"{uri}/api/v2/assets/{form_id}/data/124961136/attachments/46493730/",
                        "download_medium_url": f"{uri}/api/v2/assets/{form_id}/data/124961136/attachments/46493730/",
                        "download_small_url": f"{uri}/api/v2/assets/{form_id}/data/124961136/attachments/46493730/",
                        "mimetype": "image/jpeg",
                        "filename": "cmi_admin_kobo_test/attachments/f7bef041e8624f09946bff05ee5cbd4b/e58da38d-3eee-4bd7-8512-4a97ea8fbb01/1637106370994.jpg",
                        "instance": 124961136,
                        "xform": 815328,
                        "id": 46493730,
                    }
                ],
                "_status": "submitted_via_web",
                "_geolocation": [36.97012, -122.0109429],
                "_submission_time": "2021-11-16T23:46:27",
                "_tags": [],
                "_notes": [],
                "_validation_status": {},
                "_submitted_by": "cmi_admin_kobo_test",
            },
            {
                "_id": 125283733,
                "formhub/uuid": "f7bef041e8624f09946bff05ee5cbd4b",
                "start": "2021-11-18T07:13:40.976-06:00",
                "end": "2021-11-18T07:14:16.131-06:00",
                "today": "2021-11-18",
                "username": "iamjeffg",
                "deviceid": "collect:LNnwswilWRvq6o6r",
                "Record_your_current_location": "44.92933032 -93.22059967 230.54217529296875 5.36",
                "Estimate_height_of_your_tree_in_meters": "4",
                "I_like_this_tree_because": "beauty wildlife_habitat",
                "Take_a_photo_of_this_tree": "1637241249813.jpg",
                "__version__": "vLnrnT6Pvzgeh4DVfj6eDz",
                "meta/instanceID": "uuid:5c408d9d-6a76-4fbb-bf4e-9ed8f8e3e382",
                "_xform_id_string": form_id,
                "_uuid": "5c408d9d-6a76-4fbb-bf4e-9ed8f8e3e382",
                "_attachments": [
                    {
                        "download_url": f"{uri}/api/v2/assets/{form_id}/data/125283733/attachments/46632148/",
                        "download_large_url": f"{uri}/api/v2/assets/{form_id}/data/125283733/attachments/46632148/",
                        "download_medium_url": f"{uri}/api/v2/assets/{form_id}/data/125283733/attachments/46632148/",
                        "download_small_url": f"{uri}/api/v2/assets/{form_id}/data/125283733/attachments/46632148/",
                        "mimetype": "image/jpeg",
                        "filename": "cmi_admin_kobo_test/attachments/f7bef041e8624f09946bff05ee5cbd4b/5c408d9d-6a76-4fbb-bf4e-9ed8f8e3e382/1637241249813.jpg",
                        "instance": 125283733,
                        "xform": 815328,
                        "id": 46632148,
                    }
                ],
                "_status": "submitted_via_web",
                "_geolocation": [44.92933032, -93.22059967],
                "_submission_time": "2021-11-18T13:14:26",
                "_tags": [],
                "_notes": [],
                "_validation_status": {},
                "_submitted_by": "iamjeffg",
            },
            {
                "_id": 125340283,
                "formhub/uuid": "f7bef041e8624f09946bff05ee5cbd4b",
                "start": "2021-11-18T09:22:01.306-08:00",
                "end": "2021-11-18T09:50:29.560-08:00",
                "today": "2021-11-17",
                "username": "afleishman",
                "simserial": "simserial not found",
                "deviceid": "ee.kobotoolbox.org:pSpP4Tnigrggt7BV",
                "Record_your_current_location": "36.95751 -122.028192 17.47799301147461 10",
                "Estimate_height_of_your_tree_in_meters": "24",
                "I_like_this_tree_because": "shade wildlife_habitat beauty",
                "Take_a_photo_of_this_tree": "74B37D46-A4D4-440B-8509-6817D83BAD64-9_23_38.jpeg",
                "__version__": "vLnrnT6Pvzgeh4DVfj6eDz",
                "meta/instanceID": "uuid:6a0e819b-9d89-4b59-bd1e-38355dd2e05f",
                "meta/deprecatedID": "uuid:bf66ca42-160c-4207-8d01-af7df2852945",
                "_xform_id_string": form_id,
                "_uuid": "6a0e819b-9d89-4b59-bd1e-38355dd2e05f",
                "_attachments": [
                    {
                        "download_url": f"{uri}/api/v2/assets/{form_id}/data/125340283/attachments/46653884/",
                        "download_large_url": f"{uri}/api/v2/assets/{form_id}/data/125340283/attachments/46653884/",
                        "download_medium_url": f"{uri}/api/v2/assets/{form_id}/data/125340283/attachments/46653884/",
                        "download_small_url": f"{uri}/api/v2/assets/{form_id}/data/125340283/attachments/46653884/",
                        "mimetype": "image/jpeg",
                        "filename": "cmi_admin_kobo_test/attachments/f7bef041e8624f09946bff05ee5cbd4b/bf66ca42-160c-4207-8d01-af7df2852945/74B37D46-A4D4-440B-8509-6817D83BAD64-9_23_38.jpeg",
                        "instance": 125340283,
                        "xform": 815328,
                        "id": 46653884,
                    }
                ],
                "_status": "submitted_via_web",
                "_geolocation": [36.95751, -122.028192],
                "_submission_time": "2021-11-18T17:26:02",
                "_tags": [],
                "_notes": [],
                "_validation_status": {},
                "_submitted_by": "afleishman",
            },
        ]


def kobo_form_nested(uri, form_id, form_name="Household Survey Fixture"):
  """Form metadata with repeat groups and field-list groups for flattening tests."""
  return {
    "name": form_name,
    "uid": form_id,
    "owner__username": "fixture_user",
    "data": posixpath.join(uri, "api/v2/assets", form_id, "data/"),
    "content": {
      "schema": "1",
      "survey": [
        {
          "name": "village_name",
          "type": "text",
          "label": ["Village name"],
          "$qpath": "village_name",
          "$xpath": "village_name",
          "$autoname": "village_name",
        },
        {
          "name": "interviewer_name",
          "type": "text",
          "label": ["Interviewer name"],
          "$qpath": "interviewer_name",
          "$xpath": "interviewer_name",
          "$autoname": "interviewer_name",
        },
        {
          "name": "household_members",
          "type": "begin_repeat",
          "label": ["Household members"],
          "$qpath": "household_members",
          "$xpath": "household_members",
          "$autoname": "household_members",
        },
        {
          "name": "group_fixture_member_1",
          "type": "begin_group",
          "label": ["Member"],
          "$qpath": "household_members/group_fixture_member_1",
          "$xpath": "household_members/group_fixture_member_1",
          "$autoname": "group_fixture_member_1",
        },
        {
          "name": "group_fixture_member_1_name",
          "type": "text",
          "label": ["Member name"],
          "$qpath": "household_members/group_fixture_member_1/group_fixture_member_1_name",
          "$xpath": "household_members/group_fixture_member_1/group_fixture_member_1_name",
          "$autoname": "group_fixture_member_1_name",
        },
        {
          "name": "group_fixture_member_1_age",
          "type": "integer",
          "label": ["Member age"],
          "$qpath": "household_members/group_fixture_member_1/group_fixture_member_1_age",
          "$xpath": "household_members/group_fixture_member_1/group_fixture_member_1_age",
          "$autoname": "group_fixture_member_1_age",
        },
        {"type": "end_group"},
        {"type": "end_repeat"},
        {
          "name": "dwelling_counts",
          "type": "begin_group",
          "label": ["Dwelling counts"],
          "$qpath": "dwelling_counts",
          "$xpath": "dwelling_counts",
          "$autoname": "dwelling_counts",
        },
        {
          "name": "group_fixture_house_adults",
          "type": "integer",
          "label": ["Adults"],
          "$qpath": "dwelling_counts/group_fixture_house/group_fixture_house_adults",
          "$xpath": "dwelling_counts/group_fixture_house/group_fixture_house_adults",
          "$autoname": "group_fixture_house_adults",
        },
        {
          "name": "group_fixture_house_children",
          "type": "integer",
          "label": ["Children"],
          "$qpath": "dwelling_counts/group_fixture_house/group_fixture_house_children",
          "$xpath": "dwelling_counts/group_fixture_house/group_fixture_house_children",
          "$autoname": "group_fixture_house_children",
        },
        {"type": "end_group"},
      ],
      "choices": [],
      "settings": {"version": "1", "default_language": "English (en)"},
      "translated": ["label"],
      "translations": [None],
    },
  }


def _get_nested_submissions(uri, form_id):
  """Return synthetic submissions with repeat groups and field-list dicts."""
  return [
    {
      "_id": 900001,
      "formhub/uuid": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
      "village_name": "Village Alpha",
      "interviewer_name": "Interviewer A",
      "household_members": [
        {
          "household_members/group_fixture_member_1/group_fixture_member_1_name": "Person One",
          "household_members/group_fixture_member_1/group_fixture_member_1_age": "25",
        },
        {
          "household_members/group_fixture_member_1/group_fixture_member_1_name": "Person Two",
          "household_members/group_fixture_member_1/group_fixture_member_1_age": "30",
        },
      ],
      "summary_counts/adults": "2",
      "__version__": "fixtureVersion01",
      "meta/instanceID": "uuid:11111111-1111-1111-1111-111111111111",
      "_xform_id_string": form_id,
      "_uuid": "11111111-1111-1111-1111-111111111111",
      "_attachments": [],
      "_status": "submitted_via_web",
      "_geolocation": [10.0, 20.0],
      "_submission_time": "2026-01-15T10:00:00",
      "_tags": [],
      "_notes": [],
      "_validation_status": {},
      "_submitted_by": "fixture_user",
    },
    {
      "_id": 900002,
      "formhub/uuid": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
      "village_name": "Village Beta",
      "interviewer_name": "Interviewer B",
      "household_members": [
        {
          "household_members/group_fixture_member_1/group_fixture_member_1_name": "Person Three",
          "household_members/group_fixture_member_1/group_fixture_member_1_age": "40",
        },
      ],
      "dwelling_counts": {
        "dwelling_counts/group_fixture_house/group_fixture_house_adults": "2",
        "dwelling_counts/group_fixture_house/group_fixture_house_children": "1",
      },
      "__version__": "fixtureVersion01",
      "meta/instanceID": "uuid:22222222-2222-2222-2222-222222222222",
      "_xform_id_string": form_id,
      "_uuid": "22222222-2222-2222-2222-222222222222",
      "_attachments": [],
      "_status": "submitted_via_web",
      "_submission_time": "2026-01-15T11:00:00",
      "_tags": [],
      "_notes": [],
      "_validation_status": {},
      "_submitted_by": "fixture_user",
    },
  ]


def _paginate_submissions(uri, form_id, all_submissions, limit=100, start=0):
  total_count = len(all_submissions)
  end = start + limit
  results = all_submissions[start:end]

  next_url = None
  if end < total_count:
    next_params = {"limit": limit, "start": end}
    next_url = f"{uri}/api/v2/assets/{form_id}/data/?{urlencode(next_params)}"

  previous_url = None
  if start > 0:
    prev_start = max(0, start - limit)
    prev_params = {"limit": limit, "start": prev_start}
    previous_url = f"{uri}/api/v2/assets/{form_id}/data/?{urlencode(prev_params)}"

  return {
    "count": total_count,
    "next": next_url,
    "previous": previous_url,
    "results": results,
  }


def kobo_form_submissions(uri, form_id, limit=100, start=0):
    """
    Return paginated form submissions for testing.
    
    Parameters
    ----------
    uri : str
        The base URI of the KoboToolbox server
    form_id : str
        The form ID
    limit : int, optional
        The maximum number of results to return per page (default: 100, matching API behavior as of January 2026)
    start : int, optional
        The starting index for pagination (default: 0)
    
    Returns
    -------
    dict
        A paginated response with count, next, previous, and results
    """
    return _paginate_submissions(
        uri, form_id, _get_all_submissions(uri, form_id), limit, start
    )


def kobo_nested_form_submissions(uri, form_id, limit=100, start=0):
  """Return paginated nested-form submissions for flattening tests."""
  return _paginate_submissions(
    uri, form_id, _get_nested_submissions(uri, form_id), limit, start
  )
