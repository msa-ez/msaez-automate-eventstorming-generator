from typing import Any, Dict, Optional

from .xml_base import XmlBaseGenerator
from ..models import AssignFieldsToActionsGeneratorOutput
from ..utils import EsTraceUtil, XmlUtil

class AssignFieldsToActionsGenerator(XmlBaseGenerator):
    def __init__(self, model_name: str, model_kwargs: Optional[Dict[str, Any]] = None, client: Optional[Dict[str, Any]] = None):
        self.inputs_types_to_check = ["description", "existingActions", "missingFields"]
        super().__init__(model_name, AssignFieldsToActionsGeneratorOutput, model_kwargs, client)

    def _build_persona_info(self) -> Dict[str, str]:
        return {
            "persona": "Domain-Driven Design (DDD) Architect & Code Refactoring Specialist",
            "goal": "To logically integrate orphaned fields into an existing domain model structure, ensuring that each field is assigned to the most cohesive Aggregate or Value Object. Your primary function is to analyze a partially complete model, a list of unassigned fields, and the domain's functional requirements to make intelligent, context-aware assignments.",
            "backstory": "With years of experience in evolving large-scale domain models, I specialize in identifying the correct home for business data. I've seen countless models where an initial design missed a few details. My skill lies in my ability to understand the semantic meaning of both the existing model and the missing pieces, allowing me to place new attributes where they logically belong without violating DDD principles like high cohesion and clear aggregate boundaries. I treat the model as a living document and excel at making the necessary adjustments to ensure its integrity and clarity."
        }

    def _build_task_instruction_prompt(self) -> str:
        return """<instruction>
    <core_instructions>
        <title>Field Assignment Task</title>
        <task_description>Your task is to take a list of "missing fields" and assign each one to the most appropriate existing Aggregate or Value Object.</task_description>
        
        <input_description>
            <title>You will be given:</title>
            <item id="1">**Functional Requirements:** The business context for the domain.</item>
            <item id="2">**Existing Model Structure:** A list of Aggregates and Value Objects that have already been created, including their current properties.</item>
            <item id="3">**Missing Fields:** A list of field names that were specified in the DDL but are not yet present in the model.</item>
        </input_description>

        <guidelines>
            <title>Guidelines</title>
            <rule id="1">**Strict Definition of Invalid Fields:** The `invalid_properties` list is ONLY for text that is clearly not a field name. This includes comments (like those starting with `//`), numbers, or documentation snippets. A field should NOT be considered invalid simply because its ideal parent Aggregate or Value Object does not exist in the current model.</rule>
            <rule id="2">**Assign Every Valid Field:** You must assign every single valid field from the "Missing Fields" list to a parent in the "Existing Model Structure". If the ideal parent for a field does not exist, assign it to the most closely related existing parent. It is a critical error to place a valid field name in `invalid_properties`. All valid data fields must be integrated into the existing model.</rule>
            <rule id="3">**Choose the Best Parent:** For each valid field, analyze its name and the functional requirements to decide which Aggregate or Value Object is its most logical home. Think about which concept the field helps to describe.</rule>
            <rule id="4">**USE ONLY EXISTING PARENTS:** You MUST only assign fields to parents that already exist in the "Existing Model Structure". Do NOT create new Aggregates or Value Objects. Do NOT use arbitrary IDs or names.</rule>
            <rule id="5">**Correct ID Usage:** When assigning to an Aggregate, use the exact `aggregateId` from the Existing Model Structure as the `parent_id`. When assigning to a Value Object, use the exact `valueObjectId` from the Existing Model Structure as the `parent_id`.</rule>
            <rule id="6">**Correct Name Usage:** When assigning to an Aggregate, use the exact `aggregateName` from the Existing Model Structure as the `parent_name`. When assigning to a Value Object, use the exact `valueObjectName` from the Existing Model Structure as the `parent_name`.</rule>
            <rule id="7">**Infer Data Types:** For each field you assign, you must also infer its likely Java data type. Use common naming conventions as clues (e.g., `_id` -> `Long`, `_date` / `_at` -> `Date`, `is_` / `has_` -> `Boolean`, `amount` -> `Double`). When in doubt, default to `String`.</rule>
            <rule id="8">**Provide Source References:** For each field you assign, you MUST provide a `refs` that links the field back to the specific text in the "Functional Requirements" it was derived from. The requirements will have line numbers prepended to each line (e.g., "1: ...", "2: ...").</rule>
            <rule id="9">**Source Reference Format:** The format for `refs` is an array of Position Arrays: `[[["<start_line_number>", "<start_word_combination>"], ["<end_line_number>", "<end_word_combination>"]]]`. **Phrase Selection:** Choose 2-4 token phrases that mark a CLAUSE boundary — start_phrase = FIRST 2-3 tokens of the substantive content; end_phrase = LAST 2-3 tokens. Do NOT pick single-character phrases or particles that land mid-word or mid-Korean-character-cluster. Never end inside a word. **Skip Structural Lines:** Do NOT anchor on markdown headers (`#####`, etc.), table rows (lines starting with `|`), separator lines (`---`), or blank lines — anchor only on prose (user story narratives, Given/When/Then clauses, task entries). **Per-Field Specificity:** Each field's refs should point to the SPECIFIC clause that motivates THAT field — do NOT reuse a single broad block ref across many fields. **Zero-Length Forbidden:** start and end positions must NOT be identical. **Tight Spans:** typically one Given/When/Then block (≤3 lines) or a single sentence, NOT 20+ line ranges that include unrelated sections.</rule>
            <rule id="10">**Structure Your Output:** Group your assignments by the parent they belong to. For each parent, list all the properties you are adding to it.</rule>
            <rule id="11">**Provide Rationale:** In the `inference` section, explain your assignment decisions and why certain fields were marked as invalid. For example, "The `disposal_date` and `disposal_reason` fields were assigned to the `Book` aggregate because they directly describe the lifecycle events of a book. The field `//검수상태:'대기','승인','반려'` was marked as invalid because it is a comment, not an actual field name."</rule>
        </guidelines>

        <critical_constraint>You can ONLY use parent_id values and parent_name values that exist in the provided "Existing Model Structure". Never invent new IDs or names.</critical_constraint>
    </core_instructions>
    
    <inference_guidelines>
        <title>Inference Guidelines</title>
        <rule id="1">**Overall Strategy:** Start by stating your overall approach, which is to assign each missing field based on conceptual cohesion with the existing domain objects.</rule>
        <rule id="2">**Detailed Justification:** For each parent object that receives new fields, provide a clear and concise justification. Explain *why* those specific fields belong to that Aggregate or Value Object, referencing the functional requirements or domain concepts if necessary.</rule>
        <rule id="3">**Address Difficult Cases:** If any assignments were not straightforward, explain the trade-offs you considered and why you chose the final placement.</rule>
        <rule id="4">**Confirm Completeness:** Conclude by confirming that all fields from the input list have been successfully assigned.</rule>
    </inference_guidelines>

    <output_format>
        <title>JSON Output Format</title>
        <description>The output must be a JSON object structured as follows:</description>
        <schema>
{
    "assignments": [
        {
            "parent_type": "Aggregate",
            "parent_id": "<EXACT aggregateId from Existing Model Structure>",
            "parent_name": "<EXACT aggregateName from Existing Model Structure>",
            "properties_to_add": [
                {
                    "name": "<missing_field_name_1>",
                    "type": "<inferred_java_type>",
                    "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]]
                }
            ]
        },
        {
            "parent_type": "ValueObject",
            "parent_id": "<EXACT valueObjectId from Existing Model Structure>",
            "parent_name": "<EXACT valueObjectName from Existing Model Structure>",
            "properties_to_add": [
                {
                    "name": "<missing_field_name_2>",
                    "type": "<inferred_java_type>",
                    "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]]
                }
            ]
        }
    ],
    "invalid_properties": [
        "<invalid_property_name_1>"
    ]
}
        </schema>
    </output_format>
</instruction>"""

    def _build_json_example_input_format(self) -> Optional[Dict[str, Any]]:
        description = """# Bounded Context Overview: CourseManagement

## Role
This context is responsible for the entire lifecycle of a course, including its creation, management, and tracking. It handles course content, instructor assignments, pricing, and status changes (e.g., Draft, Published, Archived). The primary user is the Instructor.

## User Story
As an instructor, I want to create and manage my courses on the platform. When creating a course, I need to provide a title, description, and price. The course should initially be in a 'Draft' state. Once I'm ready, I can 'Publish' the course, making it available for students to enroll. If a course is outdated, I should be able to 'Archive' it, so it's no longer available for new enrollments but remains accessible to already enrolled students.

## DDL
```sql
-- Courses Table
CREATE TABLE courses (
    course_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    instructor_id BIGINT NOT NULL,
    status ENUM('DRAFT', 'PUBLISHED', 'ARCHIVED') NOT NULL DEFAULT 'DRAFT',
    price_amount DECIMAL(10, 2),
    price_currency VARCHAR(3),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_instructor_id (instructor_id)
);
```
"""
        line_numbered_description = EsTraceUtil.add_line_numbers_to_description(description)
        return {
            "functional_requirements": line_numbered_description,
            "existing_model_structure": XmlUtil.from_dict([
                {
                    "actionName": "CreateCourseAggregate",
                    "objectType": "Aggregate",
                    "ids": { "aggregateId": "agg-course" },
                    "args": {
                        "aggregateName": "Course",
                        "properties": [
                            { "name": "courseId", "type": "Long", "isKey": True },
                            { "name": "title" },
                            { "name": "status", "type": "CourseStatus" }
                        ]
                    }
                },
                {
                    "actionName": "CreateCoursePriceVO",
                    "objectType": "ValueObject",
                    "ids": { "aggregateId": "agg-course", "valueObjectId": "vo-course-price" },
                    "args": {
                        "valueObjectName": "CoursePrice",
                        "properties": [
                            { "name": "amount", "type": "Double" }
                        ]
                    }
                }
            ]),
            "missing_fields": XmlUtil.from_dict([
                "description",
                "instructor_id",
                "price_currency",
                "created_at",
                "updated_at",
                "//Course status: 'draft', 'published', 'archived'",
                "//This is a comment field"
            ])
        }

    def _build_json_example_output_format(self) -> Dict[str, Any]:
        return {
            "assignments": [
                {
                    "parent_type": "Aggregate",
                    "parent_id": "agg-course",
                    "parent_name": "Course",
                    "properties_to_add": [
                        { "name": "description", "type": "String", "refs": [[["7", "provide"], ["7", "price"]]] },
                        { "name": "instructor_id", "type": "Long", "refs": [[["16", "instructor_id"], ["16", "NULL"]]] },
                        { "name": "created_at", "type": "Date", "refs": [[["20", "created_at"], ["20", "DEFAULT"]]] },
                        { "name": "updated_at", "type": "Date", "refs": [[["21", "updated_at"], ["21", "DEFAULT"]]] }
                    ]
                },
                {
                    "parent_type": "ValueObject",
                    "parent_id": "vo-course-price",
                    "parent_name": "CoursePrice",
                    "properties_to_add": [
                        { "name": "price_currency", "type": "String", "refs": [[["19", "price_currency"], ["19", "(3)"]]] }
                    ]
                }
            ],
            "invalid_properties": [
                "//Course status: 'draft', 'published', 'archived'",
                "//This is a comment field"
            ]
        }
    
    def _build_json_user_query_input_format(self) -> Dict[str, Any]:
        inputs = self.client.get("inputs")
        return {
            "functional_requirements": inputs.get("description", ""),
            "existing_model_structure": XmlUtil.from_dict(inputs.get("existingActions")),
            "missing_fields": XmlUtil.from_dict(sorted(inputs.get("missingFields")))
        } 