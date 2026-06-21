from typing import Any, Dict, Optional

from .xml_base import XmlBaseGenerator
from ..utils import EsTraceUtil, XmlUtil
from ..models import CreateAggregateActionsByFunctionOutput

class CreateAggregateActionsByFunction(XmlBaseGenerator):
    def __init__(self, model_name: str, model_kwargs: Optional[Dict[str, Any]] = None, client: Optional[Dict[str, Any]] = None):
        self.inputs_types_to_check = ["targetBoundedContextName", "description", "targetAggregateStructure", "attributesToGenerate"]
        super().__init__(model_name, CreateAggregateActionsByFunctionOutput, model_kwargs, client)

    def _build_persona_info(self) -> Dict[str, str]:
        return {
            "persona": "Domain-Driven Design (DDD) Architect and Strategic Modeling Expert",
            "goal": "To translate complex business domains into well-structured software designs by creating precise bounded contexts, cohesive aggregates, and event-driven architectures that accurately capture domain knowledge, enforce business invariants, and provide adaptable models that evolve with changing business requirements.",
            "backstory": "Drawing on over 15 years of experience implementing complex enterprise systems across diverse industries, I've developed deep expertise in strategic and tactical domain modeling. My methodical approach balances technical implementation with business needs, emphasizing clean domain boundaries and semantic integrity. I've successfully guided organizations through complex domain transformations by identifying core concepts and designing systems that speak the language of the business. My working style prioritizes correctness, maintainability, and elegant expression of domain concepts, allowing me to navigate even the most intricate business domains with clarity and precision."
        }
        
    def _build_task_instruction_prompt(self) -> str:
        return """<instruction>
    <core_instructions>
        <title>Action Generation Task</title>
        <task_description>Based on the provided functional requirements and suggested structure, your task is to write actions to create new elements (Aggregates, ValueObjects, Enumerations) within a specified Bounded Context.</task_description>
        <meta_rule name="Priority of Additional Requirements">If an `additional_requirements` input is provided, its rules MUST be followed strictly, even if they seem to conflict with other guidelines. The instructions in `additional_requirements` take the highest precedence.</meta_rule>

        <guidelines>
            <title>Guidelines</title>
            
            <category name="Operational Guidelines">
                <rule id="op-1">Prioritize identifying and implementing ubiquitous language patterns consistently across domain models and technical implementations.</rule>
                <rule id="op-2">Apply tactical DDD patterns (aggregates, entities, value objects, repositories, domain services) with precision to solve specific domain problems.</rule>
                <rule id="op-3">Enforce strict aggregate boundaries to maintain data consistency and transaction isolation.</rule>
                <rule id="op-4">Design event streams that capture complete domain state transitions and history.</rule>
                <rule id="op-5">Recommend appropriate bounded context integration patterns based on team relationships and communication needs.</rule>
                <rule id="op-6">Balance technical constraints with business requirements to create pragmatic domain models.</rule>
                <rule id="op-7">Leverage value objects to encapsulate related attributes and validation rules.</rule>
                <rule id="op-8">Use enumerations strategically to model discrete states, categories, and classification schemes.</rule>
                <rule id="op-9">Provide clear guidance on maintaining consistency within transaction boundaries.</rule>
                <rule id="op-10">Focus on designing models that evolve gracefully with changing business requirements.</rule>
            </category>

            <category name="Data Type Rules">
                <rule id="dt-1">For Aggregate properties, use Basic Java types (String, Long, Integer, Double, Boolean, Date) or custom types defined as Enumeration or ValueObject.</rule>
                <rule id="dt-2">For collections, use the 'List<ClassName>' syntax (e.g., List<String>).</rule>
            </category>

            <category name="Type Reference and Enumeration Rules">
                <rule id="tr-1">Use Enumerations for properties representing a fixed set of values, categories, or states (e.g., names ending with Type, Status, Category, Level, Phase, Stage).</rule>
                <rule id="tr-2">Use ValueObjects for groups of related properties forming a meaningful, immutable concept. All ValueObjects must be directly associated with their Aggregate. Avoid nested or excessive ValueObjects.</rule>
            </category>

            <category name="Traceability Rules">
                <rule id="trace-0">**WHEN refs is REQUIRED vs OPTIONAL:**
                    - **REQUIRED (non-empty):** If you created this Aggregate/ValueObject/Enumeration/property because you saw SPECIFIC TEXT in the requirements (a user story, an acceptance criterion, a domain term explicitly mentioned), you MUST provide refs to that text. Empty refs here = traceability loss.
                    - **OPTIONAL (empty `[]`):** If this is an inferred standard DDD element — audit fields (`createdAt`, `updatedAt`), ID value objects, paired counterpart concepts, structural completion — with no specific clause mentioning it, return `"refs": []` HONESTLY.
                    - **NEVER fabricate refs.** Fabricated refs pointing to unrelated text are worse than empty. Empty = honest "inferred"; fabricated = lie.</rule>
                <rule id="trace-1">For every created element (Aggregate, ValueObject, Enumeration) and each of their properties, you should provide `refs` when traceable per the rule above.</rule>
                <rule id="trace-2">The `refs` must link to the "Functional Requirements" using the format: `[[["(start_line_number)", "(start_word_combination)"], ["(end_line_number)", "(end_word_combination)"]]]`.</rule>
                <rule id="trace-3">**Clause-Aligned Phrases:** Choose 2-4 token phrases that mark a meaningful clause boundary — start_phrase = FIRST 2-3 tokens of the substantive content; end_phrase = LAST 2-3 tokens. Do NOT pick single-character phrases or particles that land mid-word or mid-Korean-character-cluster. Never end inside a word.</rule>
                <rule id="trace-4">**Skip Structural Lines:** Do NOT anchor on markdown headers (lines starting with `#`, `##`, ..., `######`), table rows (lines starting with `|`), separator lines (`---`), or blank lines. Anchor only on prose lines (user story narratives `> *As a ...`, Given/When/Then clauses, task entries `- [PROJ-US-FR-...-TASK-...]`).</rule>
                <rule id="trace-5">**Per-Element Specificity:** Each element's refs should point to the SPECIFIC clause that defines THAT element. Do NOT reuse a single broad multi-line block ref for many different elements — that destroys per-element traceability.</rule>
                <rule id="trace-6">**Zero-Length Forbidden:** start and end positions must NOT be identical.</rule>
                <rule id="trace-7">**Tight Spans:** Keep refs tight — typically one Given/When/Then block (≤3 lines) or a single sentence. Avoid 20+ line ranges that include unrelated sections (table headers, other user stories, blank gaps).</rule>
            </category>

            <category name="Naming and Language Conventions">
                <rule id="name-1">Object names (classes, properties, methods, enumeration members) must be in English.</rule>
                <rule id="name-2">Supporting content (aliases, descriptions) must adhere to the preferred language setting.</rule>
                <rule id="name-3">Enumeration member names must be in UPPER_SNAKE_CASE. If the requirements specify enumeration values in a non-English language (e.g., Korean), you must translate them into meaningful English equivalents. For example, if the requirement lists '대출중', the output name should be 'ON_LOAN'.</rule>
            </category>

            <category name="Structural Rules">
                <rule id="struct-1">Aggregates must have exactly one primary key. For composite keys, use a ValueObject.</rule>
                <rule id="struct-2">Reference other Aggregates by class name, not ID.</rule>
                <rule id="struct-3">ValueObjects must be directly linked to an Aggregate and should not be nested.</rule>
                <rule id="struct-4">All generated Aggregates, ValueObjects, and Enumerations must have at least one property. Do not create elements with empty property lists.</rule>
            </category>
            
            <category name="Event-Driven and Context-Aware Design">
                <rule id="event-1">Consider how domain events influence aggregate state and properties. Ensure aggregates can produce/handle relevant events.</rule>
                <rule id="event-2">Analyze context relationships (Pub/Sub, API calls) to include properties needed for integration.</rule>
            </category>

            <category name="Creation and Constraint Guidelines">
                <rule id="create-1">Strictly create only the Aggregates listed in the 'aggregate_to_create' input. Do not invent or generate any other Aggregates, even if they seem logically related or necessary to accommodate certain properties. All generated ValueObjects and Enumerations must be directly associated with these specified Aggregates.</rule>
                <rule id="create-2">Choose specific property types over generic ones, considering event payloads.</rule>
                <rule id="constraint-1">Do not alter existing Aggregates. Do not recreate existing types. Avoid comments in the output JSON. Ensure unique names.</rule>
                <rule id="constraint-2">Every ValueObject and Enumeration must be used as a property in at least one Aggregate.</rule>
            </category>
        </guidelines>
    </core_instructions>
    
    <inference_guidelines>
        <title>Inference Guidelines</title>
        <rule id="1">The reasoning should directly inform the output result with specific design decisions rather than generic strategies.</rule>
        <rule id="2">Begin by thoroughly understanding the task requirements and the overall domain context.</rule>
        <rule id="3">Evaluate key design aspects, including domain alignment, adherence to Domain-Driven Design (DDD) principles, and technical feasibility.</rule>
        <rule id="4">Analyze the relationships and dependencies between Aggregates, ValueObjects, and Enumerations precisely.</rule>
        <rule id="5">Ensure that all design decisions comply with DDD best practices.</rule>
        <rule id="6">When properties represent state or status information, enforce the use of Enumerations to clearly define valid values.</rule>
        <rule id="7">Verify that every ValueObject and Enumeration is directly associated with an Aggregate; avoid nested or subordinate ValueObject definitions.</rule>
        <rule id="8">Avoid creating unnecessary or excessive ValueObjects; integrate properties directly into the Aggregate unless a distinct ValueObject offers significant encapsulation.</rule>
        <rule id="9">Consider domain events and their impact on aggregate design by analyzing which events the aggregate should produce or consume and ensuring properties support event-driven state transitions.</rule>
        <rule id="10">Evaluate context relationships and integration patterns (Pub/Sub, API calls) and include properties needed for external system integration.</rule>
    </inference_guidelines>
    
    <output_format>
        <title>JSON Output Format</title>
        <description>The output must be a JSON object structured as follows:</description>
        <schema>
{
    "aggregateActions": [
        {
            "actionName": "<actionName>",
            "objectType": "Aggregate",
            "ids": { "aggregateId": "<aggregateId>" },
            "args": {
                "aggregateName": "<aggregateName>",
                "aggregateAlias": "<aggregateAlias>",
                "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]],
                "properties": [
                    {
                        "name": "<propertyName>",
                        "type": "<propertyType>",
                        "isKey": <true|false>,
                        "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]]
                    }
                ]
            }
        }
    ],
    "valueObjectActions": [
        {
            "actionName": "<actionName>",
            "objectType": "ValueObject",
            "ids": { "aggregateId": "<aggregateId>", "valueObjectId": "<valueObjectId>" },
            "args": {
                "valueObjectName": "<valueObjectName>",
                "valueObjectAlias": "<valueObjectAlias>",
                "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]],
                "properties": [
                    {
                        "name": "<propertyName>",
                        "type": "<propertyType>",
                        "isKey": <true|false>,
                        "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]]
                    }
                ]
            }
        }
    ],
    "enumerationActions": [
        {
            "actionName": "<actionName>",
            "objectType": "Enumeration",
            "ids": { "aggregateId": "<aggregateId>", "enumerationId": "<enumerationId>" },
            "args": {
                "enumerationName": "<enumerationName>",
                "enumerationAlias": "<enumerationAlias>",
                "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]],
                "properties": [
                    {
                        "name": "<propertyName>",
                        "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]]
                    }
                ]
            }
        }
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

## Key Events
- CourseCreated
- CoursePublished
- CoursePriceUpdated
- CourseArchived
- StudentEnrolled

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

## Context Relations
### CourseToPayment
- **Type**: API Call
- **Direction**: calls
- **Target Context**: PaymentService
- **Reason**: When a student enrolls in a course, the CourseManagement context needs to initiate a payment process synchronously.
- **Interaction Pattern**: Makes a REST API call to the PaymentService to process the course fee."""
        line_numbered_description = EsTraceUtil.add_line_numbers_to_description(description)

        return {
            "bounded_context_to_generate_actions": "CourseManagement",
            
            "functional_requirements": line_numbered_description,
            
            "suggested_structure": XmlUtil.from_dict([
                {
                    "aggregateName": "Course",
                    "aggregateAlias": "Online Course",
                    "enumerations": [
                        {
                            "name": "CourseStatus",
                            "alias": "Course Status"
                        }
                    ],
                    "valueObjects": [
                        {
                            "name": "CoursePrice",
                            "alias": "Course Price"
                        }
                    ]
                }
            ]),
            
            "aggregate_to_create": XmlUtil.from_dict({
                "name": "Course",
                "alias": "Online Course"
            }),
        }

    def _build_json_example_output_format(self) -> Dict[str, Any]:
        return {
            "aggregateActions": [
                {
                    "actionName": "CreateCourseAggregate",
                    "objectType": "Aggregate",
                    "ids": {
                        "aggregateId": "agg-course"
                    },
                    "args": {
                        "aggregateName": "Course",
                        "aggregateAlias": "Online Course",
                        "refs": [[["4", "lifecycle"], ["4", "course"]]],
                        "properties": [
                            { "name": "courseId", "type": "Long", "isKey": True, "refs": [[["20", "course_id"], ["20", "KEY"]]] },
                            { "name": "title", "type": "String", "isKey": False, "refs": [[["7", "provide"], ["7", "title"]]] },
                            { "name": "description", "type": "String", "isKey": False, "refs": [[["7", "title"], ["7", "price"]]] },
                            { "name": "instructorId", "type": "Long", "isKey": False, "refs": [[["23", "instructor_id"], ["23", "NULL"]]] },
                            { "name": "price", "type": "CoursePrice", "isKey": False, "refs": [[["7", "description"], ["7", "price"]]] },
                            { "name": "status", "type": "CourseStatus", "isKey": False, "refs": [[["4", "status"], ["4", "changes"]]] },
                            { "name": "createdAt", "type": "Date", "isKey": False, "refs": [[["27", "created_at"], ["27", "DEFAULT"]]] },
                            { "name": "updatedAt", "type": "Date", "isKey": False, "refs": [[["28", "updated_at"], ["28", "DEFAULT"]]] }
                        ]
                    }
                }
            ],
            "valueObjectActions": [
                {
                    "actionName": "CreateCoursePriceVO",
                    "objectType": "ValueObject",
                    "ids": {
                        "aggregateId": "agg-course",
                        "valueObjectId": "vo-course-price"
                    },
                    "args": {
                        "valueObjectName": "CoursePrice",
                        "valueObjectAlias": "Course Price",
                        "refs": [[["7", "price"], ["7", "price"]]],
                        "properties": [
                            { "name": "amount", "type": "Double", "isKey": False, "refs": [[["25", "price_amount"], ["25", "(10, 2)"]]] },
                            { "name": "currency", "type": "String", "isKey": False, "refs": [[["26", "price_currency"], ["26", "(3)"]]] }
                        ]
                    }
                }
            ],
            "enumerationActions": [
                {
                    "actionName": "CreateCourseStatusEnum",
                    "objectType": "Enumeration",
                    "ids": {
                        "aggregateId": "agg-course",
                        "enumerationId": "enum-course-status"
                    },
                    "args": {
                        "enumerationName": "CourseStatus",
                        "enumerationAlias": "Course Status",
                        "refs": [[["4", "status"], ["4", "status"]]],
                        "properties": [
                            { "name": "DRAFT", "refs": [[["7", "Draft"], ["7", "state"]]] },
                            { "name": "PUBLISHED", "refs": [[["7", "Publish"], ["7", "course"]]] },
                            { "name": "ARCHIVED", "refs": [[["7", "Archive"], ["7", "it"]]] }
                        ]
                    }
                }
            ]
        }
    
    def _build_json_user_query_input_format(self) -> Dict[str, Any]:
        inputs = self.client.get("inputs")
        
        additional_requirements = f""
        extracted_ddl_fields = inputs.get("attributesToGenerate")
        if extracted_ddl_fields:
            fields_str = "<fields>"
            for field in extracted_ddl_fields:
                fields_str += f"<field>{field}</field>"
            fields_str += "</fields>"
            additional_requirements += f"""<category name="DDL Field Requirement">
    <rule>The following fields are mandatory and MUST be included within the Aggregates or ValueObjects specified for creation in 'aggregate_to_create'. This is a strict directive that overrides all other guidelines, including Domain-Driven Design principles like aggregate boundary integrity. You must find the most relevant place for these fields within the specified structures. The names of these fields MUST be used exactly as provided in the `<fields>` list, without any modification, prefix, or suffix. DO NOT create new Aggregates. If a perfect fit is not obvious, add them to the most closely related property group. Your primary task is to ensure all listed fields are present in the output, confined to the specified aggregates.</rule>
    {fields_str}
</category>
"""
        
        return {
            "bounded_context_to_generate_actions": inputs.get("targetBoundedContextName"),

            "functional_requirements": inputs.get("description"),

            "suggested_structure": XmlUtil.from_dict(inputs.get("targetAggregateStructure")),

            "aggregate_to_create": XmlUtil.from_dict({
                "aggregateName": inputs.get("targetAggregateStructure").get("aggregateName"),
                "aggregateAlias": inputs.get("targetAggregateStructure").get("aggregateAlias")
            }),

            "additional_requirements": additional_requirements,
        }