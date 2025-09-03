# from __future__ import annotations
#
# from typing import List, Optional, Set
# from repositories.participant_repository import ParticipantRepository
# from repositories.country_repository import CountryRepository
# from domain.models.participant import Participant, Grade
#
#
# class ParticipantService:
#     """Service layer for participant operations with country validation."""
#
#     def __init__(self):
#         self.participant_repo = ParticipantRepository()
#         self.country_repo = CountryRepository()
#         self._valid_country_cids: Optional[Set[str]] = None
#
#     def get_valid_country_cids(self) -> Set[str]:
#         """Get all valid country CIDs from the database."""
#         if self._valid_country_cids is None:
#             countries = self.country_repo.find_all()
#             self._valid_country_cids = {country.cid for country in countries}
#         return self._valid_country_cids
#
#     def validate_participant_countries(self, participant: Participant) -> bool:
#         """Validate that all country references in a participant are valid."""
#         valid_cids = self.get_valid_country_cids()
#         return participant.validate_country_references(valid_cids)
#
#     def create_participant(self, participant_data: dict) -> Optional[Participant]:
#         """Create a new participant with country validation."""
#         participant = Participant(**participant_data)
#
#         if not self.validate_participant_countries(participant):
#             return None
#
#         self.participant_repo.save(participant)
#         return participant
#
#     def bulk_create_participants(self, participants_data: List[dict]) -> List[Participant]:
#         """Create multiple participants with country validation."""
#         valid_participants = []
#         valid_cids = self.get_valid_country_cids()
#
#         for data in participants_data:
#             try:
#                 participant = Participant(**data)
#                 if participant.validate_country_references(valid_cids):
#                     valid_participants.append(participant)
#             except Exception as e:
#                 print(f"Error creating participant: {e}")
#
#         if valid_participants:
#             self.participant_repo.bulk_save(valid_participants)
#
#         return valid_participants
#
#     def update_participant_grade(self, pid: str, grade: Grade) -> bool:
#         """Update a participant's grade."""
#         return self.participant_repo.update_grade(pid, grade)
#
#     def get_participants_by_country(self, country_cid: str) -> List[Participant]:
#         """Get all participants associated with a country."""
#         return self.participant_repo.find_by_country(country_cid)
#
#     def get_participants_by_grade(self, grade: Grade) -> List[Participant]:
#         """Get all participants with a specific grade."""
#         return self.participant_repo.find_by_grade(grade)