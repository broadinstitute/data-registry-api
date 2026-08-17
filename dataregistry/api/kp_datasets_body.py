"""Compose kp_datasets body HTML from structured inputs.

Reproduces the section pattern the Drupal authors typed by hand
(<h3>Publication</h3> / <h3>Phenotypes</h3> / <h3>Experiment summary</h3>)
so generated and migrated bodies render identically on the portals.
User-supplied text is HTML-escaped; migrated Drupal bodies are stored
verbatim and never pass through here unless re-saved via the edit flow.
"""
import html


def compose_body(publication, phenotype_names, experiment_summary):
    pub = html.escape(publication) if publication else 'Unpublished'
    items = ''.join(f'<li>{html.escape(p)}</li>' for p in phenotype_names)
    summary = html.escape(experiment_summary or '')
    return (f'<h3>Publication</h3><p>{pub}</p>'
            f'<h3>Phenotypes</h3><ul>{items}</ul>'
            f'<h3>Experiment summary</h3><p>{summary}</p>')
