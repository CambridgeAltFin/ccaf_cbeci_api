from google.cloud.firestore_v1 import CollectionReference
from firebase_admin import firestore
from services.firebase import Collections


class RealtimeCollection:

    def __init__(self, collection):
        super().__init__()
        if isinstance(collection, CollectionReference):
            self.collectionRf = collection
        else:
            self.collectionRf = firestore.client().collection(collection)
        self._docs = {}
        self._unsubscribe = None
        self._loaded = False

        self.init()

    def init(self):
        self._subscribe()

    def get(self, doc_id=None):
        if doc_id is None:
            return list(self._docs.values())

        if doc_id in self._docs:
            return self._docs[doc_id]

        return None

    @property
    def is_loaded(self):
        return self._loaded

    def _subscribe(self):
        def on_snapshot_listener(collection_snapshot, changes, read_time):
            if self._loaded is False:
                self._loaded = True

            for change in changes:
                document = change.document
                if change.type.name == 'REMOVED':
                    del self._docs[document.id]
                else:
                    self._docs[document.id] = document.to_dict()

        self._unsubscribe = self.collectionRf.on_snapshot(on_snapshot_listener)

    def unsubscribe(self):
        if callable(self._unsubscribe):
            self._unsubscribe()
            self._unsubscribe = None

        self._loaded = False
        self._docs = {}


class RealtimeCollections:
    def __init__(self):
        self._initialized = False
        self._instance = None
        self.collections = {}

    def init(self, collections=None):
        if self._initialized:
            return

        if collections is None:
            collections = [Collections.TEXT_PAGES, Collections.REPORTS, Collections.SPONSORS]
        for collection in collections:
            self.collections[collection] = RealtimeCollection(collection=collection)

        self._initialized = True


realtime_collections = RealtimeCollections()
