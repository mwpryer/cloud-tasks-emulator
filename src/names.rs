pub(crate) const LOCATION: &[&str] = &["projects", "locations"];
pub(crate) const QUEUE: &[&str] = &["projects", "locations", "queues"];
pub(crate) const TASK: &[&str] = &["projects", "locations", "queues", "tasks"];

/// IDs of a name made of exactly these `collection/id` pairs, in order
pub(crate) fn ids<'a>(name: &'a str, collections: &[&str]) -> Option<Vec<&'a str>> {
    let segments: Vec<&str> = name.split('/').collect();
    let matches = segments.len() == 2 * collections.len()
        && segments
            .chunks(2)
            .zip(collections)
            .all(|(pair, collection)| pair[0] == *collection && !pair[1].is_empty());
    matches.then(|| segments.into_iter().skip(1).step_by(2).collect())
}

fn is_id(id: &str, max_len: usize, extra: &[char]) -> bool {
    !id.is_empty()
        && id.len() <= max_len
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || extra.contains(&c))
}

pub(crate) fn is_queue_id(id: &str) -> bool {
    is_id(id, 100, &['-'])
}

pub(crate) fn is_task_id(id: &str) -> bool {
    is_id(id, 500, &['-', '_'])
}

pub(crate) fn last_segment(name: &str) -> &str {
    name.rsplit_once('/').map_or(name, |(_, id)| id)
}
