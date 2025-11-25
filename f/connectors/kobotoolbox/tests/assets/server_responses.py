import posixpath
from urllib.parse import urlencode


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


def kobo_form_submissions(uri, form_id, limit=None, start=0):
    """
    Return paginated form submissions for testing.
    
    Parameters
    ----------
    uri : str
        The base URI of the KoboToolbox server
    form_id : str
        The form ID
    limit : int, optional
        The maximum number of results to return per page
    start : int, optional
        The starting index for pagination (default: 0)
    
    Returns
    -------
    dict
        A paginated response with count, next, previous, and results
    """
    all_submissions = _get_all_submissions(uri, form_id)
    total_count = len(all_submissions)
    
    # If no limit specified, return all results
    if limit is None:
        return {
            "count": total_count,
            "next": None,
            "previous": None,
            "results": all_submissions,
        }
    
    # Paginate results
    end = start + limit
    results = all_submissions[start:end]
    
    # Build next URL if there are more results
    next_url = None
    if end < total_count:
        next_params = {"limit": limit, "start": end}
        next_url = f"{uri}/api/v2/assets/{form_id}/data/?{urlencode(next_params)}"
    
    # Build previous URL if we're not on the first page
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
