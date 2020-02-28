/// Datastore `id`, scored according to search performance
pub struct ScoredId {
    pub id: u64,
    pub score: f32,
}

/// Like `tantivy`'s `TopDocs` collector, but without any limit
///
/// Returns `ScoredId`, a container for the datastore `id` and search score.
pub struct ScoredIds {
    pub size_hint: Option<usize>,
    pub id_field: tantivy::schema::Field,
}

// Used by the `ScoredIds` collector.
#[doc(hidden)]
pub struct ScoredIdsSegmentCollector {
    id_field_reader: Option<tantivy::fastfield::FastFieldReader<u64>>,
    buffer: Vec<ScoredId>,
}

impl tantivy::collector::Collector for ScoredIds {
    type Fruit = Vec<ScoredId>;
    type Child = ScoredIdsSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentLocalId,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(ScoredIdsSegmentCollector {
            buffer: self.size_hint.map(Vec::with_capacity).unwrap_or_else(Vec::new),
            id_field_reader: segment.fast_fields().u64(self.id_field.clone()),
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(&self, segment_fruits: Vec<Self::Fruit>) -> tantivy::Result<Self::Fruit> {
        let mut out = segment_fruits.into_iter().flatten().collect::<Vec<_>>();
        out.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or_else(|| a.id.cmp(&b.id)));
        Ok(out)
    }
}

impl tantivy::collector::SegmentCollector for ScoredIdsSegmentCollector {
    type Fruit = Vec<ScoredId>;

    fn collect(&mut self, doc: tantivy::DocId, score: tantivy::Score) {
        if let Some(ref id_field_reader) = self.id_field_reader {
            self.buffer.push(ScoredId { score, id: id_field_reader.get(doc) });
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.buffer
    }
}
