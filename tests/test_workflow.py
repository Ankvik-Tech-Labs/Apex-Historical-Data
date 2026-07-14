from pathlib import Path


WORKFLOW = Path(__file__).resolve().parents[1] / ".github/workflows/collect.yml"


def test_workflow_uses_immutable_dated_releases():
    text = WORKFLOW.read_text()

    assert "gh release delete" not in text
    assert "refs/tags/latest" not in text
    assert "--prior-data-dir" not in text
    assert '--until ${{ needs.setup.outputs.collection_cutoff }}' in text
    assert 'gh release view "$SNAPSHOT_TAG"' in text
    assert 'gh release create "${{ needs.setup.outputs.snapshot_tag }}"' in text
