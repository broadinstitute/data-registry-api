import pytest
from fastapi.testclient import TestClient
from uuid import uuid4
from sqlalchemy.exc import IntegrityError

from dataregistry.api.model import SGCCohort, SGCCohortFile
from dataregistry.api import query
from dataregistry.api.db import DataRegistryReadWriteDB
from sqlalchemy import text


def test_upsert_sgc_cohort_insert_new(api_client: TestClient):
    """Test inserting a new cohort at the query level"""
    engine = DataRegistryReadWriteDB().get_engine()
    
    cohort = SGCCohort(
        name="Test Cohort",
        uploaded_by="testuser",
        total_sample_size=1000,
        number_of_males=500,
        number_of_females=500
    )
    
    cohort_id = query.upsert_sgc_cohort(engine, cohort)
    
    assert cohort_id is not None
    assert len(cohort_id) == 32  # UUID without dashes
    
    # Verify it was inserted
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT name, uploaded_by, total_sample_size FROM sgc_cohorts WHERE id = :cohort_id"),
            {"cohort_id": cohort_id}
        ).fetchone()
        assert result is not None
        assert result[0] == "Test Cohort"
        assert result[1] == "testuser"
        assert result[2] == 1000


def test_upsert_sgc_cohort_update_existing(api_client: TestClient):
    """Test updating existing cohort by providing ID"""
    engine = DataRegistryReadWriteDB().get_engine()
    
    # Insert initial cohort
    cohort = SGCCohort(
        name="Test Cohort",
        uploaded_by="testuser",
        total_sample_size=1000,
        number_of_males=500,
        number_of_females=500
    )
    
    cohort_id = query.upsert_sgc_cohort(engine, cohort)
    
    # Update cohort by providing the ID
    updated_cohort = SGCCohort(
        id=cohort_id,  # Provide ID to trigger update
        name="Test Cohort",
        uploaded_by="testuser",
        total_sample_size=1500,  # Different values
        number_of_males=700,
        number_of_females=800
    )
    
    updated_id = query.upsert_sgc_cohort(engine, updated_cohort)
    
    # Should return same ID
    assert updated_id == cohort_id
    
    # Verify there's still only one record and values were updated
    with engine.connect() as conn:
        results = conn.execute(
            text("SELECT HEX(id), total_sample_size, number_of_males, number_of_females FROM sgc_cohorts WHERE name = :name AND uploaded_by = :uploaded_by"),
            {"name": "Test Cohort", "uploaded_by": "testuser"}
        ).fetchall()
        
        assert len(results) == 1  # Should be only one record
        row = results[0]
        assert row[1] == 1500  # Values should be updated
        assert row[2] == 700
        assert row[3] == 800


def test_upsert_sgc_cohort_with_provided_id(api_client: TestClient):
    """Test upserting cohort with provided ID at the query level"""
    engine = DataRegistryReadWriteDB().get_engine()
    
    provided_id = str(uuid4())
    cohort = SGCCohort(
        id=provided_id,
        name="Test Cohort with ID",
        uploaded_by="testuser",
        total_sample_size=2000,
        number_of_males=1000,
        number_of_females=1000
    )
    
    cohort_id = query.upsert_sgc_cohort(engine, cohort)
    
    # Should use the provided ID
    assert cohort_id == str(provided_id).replace('-', '')


def test_upsert_sgc_cohort_invalid_data(api_client: TestClient):
    """Test error handling with invalid cohort data"""
    from pydantic import ValidationError
    
    # The Pydantic model should catch invalid data before it gets to the database
    with pytest.raises(ValidationError):
        invalid_cohort = SGCCohort(
            name="Test Cohort",
            uploaded_by="testuser",
            total_sample_size=None,  # Should fail validation - required field
            number_of_males=500,
            number_of_females=500
        )


def test_upsert_sgc_cohort_duplicate_name_error(api_client: TestClient):
    """Test that duplicate name for same user raises IntegrityError when no ID provided"""
    engine = DataRegistryReadWriteDB().get_engine()
    
    # Insert initial cohort
    cohort = SGCCohort(
        name="Duplicate Name Test",
        uploaded_by="testuser",
        total_sample_size=1000,
        number_of_males=500,
        number_of_females=500
    )
    
    cohort_id = query.upsert_sgc_cohort(engine, cohort)
    assert cohort_id is not None
    
    # Try to create another cohort with same name+uploaded_by (no ID provided)
    duplicate_cohort = SGCCohort(
        name="Duplicate Name Test",  # Same name and uploaded_by
        uploaded_by="testuser",
        total_sample_size=1500,
        number_of_males=700,
        number_of_females=800
    )
    
    # Should raise IntegrityError
    with pytest.raises(IntegrityError):
        query.upsert_sgc_cohort(engine, duplicate_cohort)


def test_upsert_sgc_cohort_different_users_same_name(api_client: TestClient):
    """Test that different users can have cohorts with the same name"""
    engine = DataRegistryReadWriteDB().get_engine()
    
    # Create cohort for first user
    cohort1 = SGCCohort(
        name="Popular Cohort Name",
        uploaded_by="user1",
        total_sample_size=1000,
        number_of_males=500,
        number_of_females=500
    )
    
    cohort_id1 = query.upsert_sgc_cohort(engine, cohort1)
    
    # Create cohort with same name for different user (should be allowed)
    cohort2 = SGCCohort(
        name="Popular Cohort Name",  # Same name
        uploaded_by="user2",  # Different user
        total_sample_size=2000,
        number_of_males=1000,
        number_of_females=1000
    )
    
    cohort_id2 = query.upsert_sgc_cohort(engine, cohort2)
    
    # Should have different IDs
    assert cohort_id1 != cohort_id2
    
    # Verify both records exist
    with engine.connect() as conn:
        results = conn.execute(
            text("SELECT uploaded_by, total_sample_size FROM sgc_cohorts WHERE name = :name ORDER BY uploaded_by"),
            {"name": "Popular Cohort Name"}
        ).fetchall()
        
        assert len(results) == 2
        assert results[0][0] == "user1"
        assert results[0][1] == 1000
        assert results[1][0] == "user2"
        assert results[1][1] == 2000


def test_insert_sgc_cohort_file(api_client: TestClient):
    """Test inserting a new cohort file"""
    engine = DataRegistryReadWriteDB().get_engine()
    
    # First create a cohort
    cohort = SGCCohort(
        name="Test Cohort for Files",
        uploaded_by="testuser",
        total_sample_size=1000,
        number_of_males=500,
        number_of_females=500
    )
    cohort_id = query.upsert_sgc_cohort(engine, cohort)
    
    # Now create a file for this cohort
    cohort_file = SGCCohortFile(
        cohort_id=cohort_id,
        file_type="cases_controls",
        file_path="/data/cohort1/cases_controls.csv",
        file_name="cases_controls.csv",
        file_size=1024000
    )
    
    file_id = query.insert_sgc_cohort_file(engine, cohort_file)
    
    assert file_id is not None
    assert len(file_id) == 32  # UUID without dashes
    
    # Verify it was inserted
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT file_type, file_path, file_name, file_size FROM sgc_cohort_files WHERE id = :file_id"),
            {"file_id": file_id}
        ).fetchone()
        assert result is not None
        assert result[0] == "cases_controls"
        assert result[1] == "/data/cohort1/cases_controls.csv"
        assert result[2] == "cases_controls.csv"
        assert result[3] == 1024000


def test_get_sgc_cohort_files(api_client: TestClient):
    """Test getting files for a cohort"""
    engine = DataRegistryReadWriteDB().get_engine()
    
    # Create a cohort
    cohort = SGCCohort(
        name="Test Cohort for File List",
        uploaded_by="testuser",
        total_sample_size=1000,
        number_of_males=500,
        number_of_females=500
    )
    cohort_id = query.upsert_sgc_cohort(engine, cohort)
    
    # Create multiple files for this cohort
    file1 = SGCCohortFile(
        cohort_id=cohort_id,
        file_type="cases_controls",
        file_path="/data/cohort1/cases_controls.csv",
        file_name="cases_controls.csv",
        file_size=1024000
    )
    
    file2 = SGCCohortFile(
        cohort_id=cohort_id,
        file_type="cooccurrence",
        file_path="/data/cohort1/cooccurrence.csv",
        file_name="cooccurrence.csv",
        file_size=2048000
    )
    
    query.insert_sgc_cohort_file(engine, file1)
    query.insert_sgc_cohort_file(engine, file2)
    
    # Get files for this cohort
    files = query.get_sgc_cohort_files(engine, cohort_id)
    
    assert len(files) == 2
    file_types = [f['file_type'] for f in files]
    assert "cases_controls" in file_types
    assert "cooccurrence" in file_types


def test_delete_sgc_cohort_file(api_client: TestClient):
    """Test deleting a cohort file"""
    engine = DataRegistryReadWriteDB().get_engine()
    
    # Create a cohort
    cohort = SGCCohort(
        name="Test Cohort for File Delete",
        uploaded_by="testuser",
        total_sample_size=1000,
        number_of_males=500,
        number_of_females=500
    )
    cohort_id = query.upsert_sgc_cohort(engine, cohort)
    
    # Create a file
    cohort_file = SGCCohortFile(
        cohort_id=cohort_id,
        file_type="cases_controls",
        file_path="/data/cohort1/cases_controls.csv",
        file_name="cases_controls.csv",
        file_size=1024000
    )
    
    file_id = query.insert_sgc_cohort_file(engine, cohort_file)
    
    # Verify file exists
    files_before = query.get_sgc_cohort_files(engine, cohort_id)
    assert len(files_before) == 1
    
    # Delete the file
    deleted = query.delete_sgc_cohort_file(engine, file_id)
    assert deleted is True
    
    # Verify file is gone
    files_after = query.get_sgc_cohort_files(engine, cohort_id)
    assert len(files_after) == 0
    
    # Try to delete non-existent file
    deleted_again = query.delete_sgc_cohort_file(engine, file_id)
    assert deleted_again is False


def test_sgc_cohort_file_duplicate_constraint(api_client: TestClient):
    """Test that duplicate file_type for same cohort raises error"""
    engine = DataRegistryReadWriteDB().get_engine()
    
    # Create a cohort
    cohort = SGCCohort(
        name="Test Cohort for Duplicate",
        uploaded_by="testuser",
        total_sample_size=1000,
        number_of_males=500,
        number_of_females=500
    )
    cohort_id = query.upsert_sgc_cohort(engine, cohort)
    
    # Create first file
    file1 = SGCCohortFile(
        cohort_id=cohort_id,
        file_type="cases_controls",
        file_path="/data/cohort1/cases_controls_v1.csv",
        file_name="cases_controls_v1.csv",
        file_size=1024000
    )
    
    query.insert_sgc_cohort_file(engine, file1)
    
    # Try to create another file with same file_type (should fail)
    file2 = SGCCohortFile(
        cohort_id=cohort_id,
        file_type="cases_controls",  # Same file_type
        file_path="/data/cohort1/cases_controls_v2.csv",
        file_name="cases_controls_v2.csv",
        file_size=2048000
    )
    
    with pytest.raises(IntegrityError):
        query.insert_sgc_cohort_file(engine, file2)


def test_sgc_cohort_file_foreign_key_constraint(api_client: TestClient):
    """Test that file for non-existent cohort raises error"""
    engine = DataRegistryReadWriteDB().get_engine()
    
    # Try to create file for non-existent cohort
    fake_cohort_id = str(uuid4()).replace('-', '')
    cohort_file = SGCCohortFile(
        cohort_id=fake_cohort_id,
        file_type="cases_controls",
        file_path="/data/fake/cases_controls.csv",
        file_name="cases_controls.csv",
        file_size=1024000
    )
    
    with pytest.raises(IntegrityError):
        query.insert_sgc_cohort_file(engine, cohort_file)
