from dataregistry.api.kp_datasets_body import compose_body, parse_experiment_summary


def test_matches_drupal_section_pattern():
    body = compose_body('Unpublished', ['Triglyceride to HDL levels'],
                        'This study conducted an East Asian ancestry GWAS of '
                        'triglyceride to HDL levels in 3971 individuals.')
    assert body == ('<h3>Publication</h3><p>Unpublished</p>'
                    '<h3>Phenotypes</h3><ul><li>Triglyceride to HDL levels</li></ul>'
                    '<h3>Experiment summary</h3><p>This study conducted an East Asian '
                    'ancestry GWAS of triglyceride to HDL levels in 3971 individuals.</p>')


def test_empty_publication_defaults_to_unpublished():
    assert '<h3>Publication</h3><p>Unpublished</p>' in compose_body(None, [], 's')
    assert '<h3>Publication</h3><p>Unpublished</p>' in compose_body('', [], 's')


def test_multiple_phenotypes_and_empty_list():
    assert '<ul><li>A</li><li>B</li></ul>' in compose_body(None, ['A', 'B'], 's')
    assert '<ul></ul>' in compose_body(None, [], 's')


def test_user_text_is_escaped():
    body = compose_body('Smith & Jones <2024>', ['T2D & obesity'], 'a < b')
    assert 'Smith &amp; Jones &lt;2024&gt;' in body
    assert 'T2D &amp; obesity' in body
    assert 'a &lt; b' in body
    assert '<2024>' not in body


def test_parse_experiment_summary_round_trips_generated_body():
    assert parse_experiment_summary(compose_body('P & Q', ['A'], 'a < b')) == 'a < b'


def test_parse_experiment_summary_returns_none_for_non_generated_body():
    assert parse_experiment_summary('<h3>Data Links</h3><p>x</p>') is None
