from typing import Any, Dict, Optional
from .xml_base import XmlBaseGenerator
from ..utils import ESValueSummarizeWithFilter, EsTraceUtil, XmlUtil
from ..models import CreatePolicyActionsByFunctionOutput

class CreatePolicyActionsByFunction(XmlBaseGenerator):
    def __init__(self, model_name: str, model_kwargs: Optional[Dict[str, Any]] = None, client: Optional[Dict[str, Any]] = None):
        self.inputs_types_to_check = ["summarizedESValue", "description"]
        super().__init__(model_name, CreatePolicyActionsByFunctionOutput, model_kwargs, client)

    def _build_persona_info(self) -> Dict[str, str]:
        return {
            "persona": "Policy Designer and Domain-Driven Design (DDD) Architect",
            "goal": "To analyze business requirements and event-driven systems to derive optimal policies, automation rules, and integration patterns that maximize business value while maintaining system integrity.",
            "backstory": "With over 15 years of hands-on experience in event-driven architectures, I have designed robust policy systems across numerous industries. My approach focuses on balancing technical precision with business needs, emphasizing maintainable implementations that respect domain boundaries while ensuring long-term scalability."
        }

    def _build_task_instruction_prompt(self) -> str:
        return """<instruction>
    <core_instructions>
        <title>Policy Derivation Task</title>
        <task_description>You need to analyse a given event storming model to derive a policy (where events trigger other events).</task_description>
        
        <input_description>
            <title>You will be given:</title>
            <item id="1">**Functional Requirements:** The business context for the domain.</item>
            <item id="2">**Summarized Existing EventStorming Model:** A summary of the existing domain model including Bounded Contexts, Aggregates, Commands, and Events.</item>
        </input_description>
    </core_instructions>
    
    <guidelines>
        <title>Policy Creation Guidelines</title>
        <rule id="1">Analyze the given requirements and event streams to derive business logic and create policies connecting events to subsequent events.</rule>
        <rule id="2">The policy name should be in English, while the alias and other content should be in user's preferred language for user readability.</rule>
        <rule id="3">Do not write comments in the output JSON object.</rule>
        <rule id="4">Each policy must have a clear, action-oriented name and a reason explaining its business value.</rule>
        <rule id="5">Consider business rules, domain events, context relations, eventual consistency, error handling, and performance when creating policies.</rule>
        <rule id="6">Leverage provided domain events and context relations to understand event triggers, cross-context flows, and interaction patterns, ensuring policies respect bounded context boundaries.</rule>
        <rule id="7">For every created policy, you MUST provide `refs` linking it to the specific text in the "Functional Requirements".
            - The format is `[[["<start_line_number>", "<start_word>"], ["<end_line_number>", "<end_word>"]]]`.
            - Use minimal (1-2) words to uniquely identify the position.
            - If a policy is inferred from multiple places, add multiple Position Arrays.
        </rule>
    </guidelines>

    <critical_validation_rules>
        <title>CRITICAL POLICY VALIDATION RULES - STRICTLY ENFORCE</title>
        <rule id="1">**NO SELF-TRIGGERING**: The event ID in `fromEventId` MUST NEVER appear in `toEventIds` for the same policy. Events cannot trigger themselves.</rule>
        <rule id="2">**NO SAME-AGGREGATE POLICIES**: Policies must only connect events between DIFFERENT aggregates. Logic within the same aggregate must be handled internally.</rule>
        <rule id="3">**MANDATORY TARGET EVENTS**: Every policy MUST have at least one event in `toEventIds`. Empty `toEventIds` arrays are forbidden.</rule>
        <rule id="4">**CROSS-BOUNDARY ONLY**: Policies should primarily connect events across bounded contexts or aggregates to implement business workflows.</rule>
        <rule id="5">**NO CIRCULAR DEPENDENCIES**: If event A triggers event B, then event B should NOT trigger event A in another policy.</rule>
        <rule id="6">**CREATE NEW EVENTS**: Policies must create NEW events, not republish the same event. The event from `fromEventId` MUST NOT be in `toEventIds`.</rule>
        <rule id="7">**AVOID**: Tightly coupled or complex policies, deadlocks, infinite loops, ambiguous names, and policies that contradict defined context relationships.</rule>
        <rule id="8">**DUPLICATE CHECK**: Do not create a duplicate policy if one already exists connecting the same set of input and output events.</rule>
    </critical_validation_rules>

    <inference_guidelines>
        <title>Inference Guidelines</title>
        <rule id="1">**Context Analysis**: Analyze the event storming model and functional requirements to understand business objectives and domain boundaries.</rule>
        <rule id="2">**Policy Design**: Derive clear policies connecting source and target events, ensuring each delivers unique business value.</rule>
        <rule id="3">**Validation**: Verify that policies avoid self-triggers, same-aggregate triggers, and circular dependencies. Policies where a source and target event belong to the same aggregate are strictly forbidden.</rule>
        <rule id="4">**Source Reference Justification**: For each policy, provide `refs` linking back to the functional requirements. **Phrase Selection:** Choose 2-4 token phrases that align with CLAUSE boundaries — start_phrase = FIRST 2-3 tokens of the substantive content; end_phrase = LAST 2-3 tokens. Do NOT pick single-character phrases or particles that land mid-word. Never end inside a word. **Skip Structural Lines:** Do NOT anchor on markdown headers (`#####`, etc.), table rows (`|`), separator lines (`---`), or blank lines. Anchor only on prose lines (user story narratives, Given/When/Then clauses, task entries). **Per-Policy Specificity:** Each policy's refs should point to its OWN specific clause — do not reuse a single broad block ref for many policies. **Zero-Length Forbidden:** start and end positions must NOT be identical. **Tight Spans:** typically one Given/When/Then block (≤3 lines), avoid 20+ line ranges.</rule>
    </inference_guidelines>
    
    <output_format>
        <title>JSON Output Format</title>
        <description>The output must be a JSON object structured as follows:</description>
        <schema>
{
    "extractedPolicies": [
        {
            "name": "<name>",
            "alias": "<alias>",
            "reason": "<reason>",
            "fromEventId": "<fromEventId>",
            "toEventIds": ["<toEventId1>", "toEventId2>"],
            "refs": [[["<start_line_number>", "<minimal_start_phrase>"], ["<end_line_number>", "<minimal_end_phrase>"]]]
        }
    ]
}
        </schema>
    </output_format>
</instruction>"""

    def _build_json_example_input_format(self) -> Optional[Dict[str, Any]]:
        description =  """# Functional Requirements: Restaurant Reservation System

## User Story
As a customer, I want to make a restaurant reservation and receive confirmation.
- Acceptance Criteria:
  - Reservation should be created with customer details and party size.
  - System should automatically assign an appropriate table.
  - Kitchen should be notified for preparation.
  - Customer should receive a confirmation.

## Key Domain Events
- `ReservationCreated`: A new reservation is created by the customer.
- `TableAssigned`: A table has been assigned to the reservation.
- `KitchenNotified`: The kitchen has been notified for preparation.
- `ReservationConfirmed`: The reservation is fully confirmed.

## DDL Snippet
```sql
CREATE TABLE reservations (
    reservation_id INT PRIMARY KEY,
    customer_id INT,
    party_size INT,
    status VARCHAR(50) -- 'PENDING', 'CONFIRMED'
);
```

## Context Relations
- **Name**: ReservationToKitchen
- **Type**: Pub/Sub
- **Interaction**: Reservation Service publishes `ReservationConfirmed` events. The Kitchen Service subscribes to trigger preparation.
- **Reason**: The kitchen needs to prepare for confirmed reservations.
"""
        description_with_line_numbers = EsTraceUtil.add_line_numbers_to_description(description)
        
        return {
            "summarized_es_value": XmlUtil.from_dict({
                "deletedProperties": ESValueSummarizeWithFilter.KEY_FILTER_TEMPLATES["aggregateInnerStickers"] + 
                    ESValueSummarizeWithFilter.KEY_FILTER_TEMPLATES["detailedProperties"],
                "boundedContexts": [
                    {
                        "id": "bc-reservation",
                        "name": "reservationservice",
                        "actors": [
                            {
                                "id": "act-customer",
                                "name": "Customer"
                            },
                            {
                                "id": "act-staff",
                                "name": "RestaurantStaff"
                            }
                        ],
                        "aggregates": [
                            {
                                "id": "agg-reservation",
                                "name": "Reservation",
                                "policies": [],
                                "commands": [
                                    {
                                        "id": "cmd-create-reservation",
                                        "name": "CreateReservation",
                                        "api_verb": "POST",
                                        "outputEvents": [
                                            {
                                                "id": "evt-reservation-created",
                                                "name": "ReservationCreated"
                                            }
                                        ]
                                    },
                                    {
                                        "id": "cmd-confirm-reservation",
                                        "name": "ConfirmReservation",
                                        "api_verb": "PATCH",
                                        "outputEvents": [
                                            {
                                                "id": "evt-reservation-confirmed",
                                                "name": "ReservationConfirmed"
                                            }
                                        ]
                                    }
                                ],
                                "events": [
                                    {
                                        "id": "evt-reservation-created",
                                        "name": "ReservationCreated",
                                    },
                                    {
                                        "id": "evt-reservation-confirmed",
                                        "name": "ReservationConfirmed",
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "id": "bc-kitchen",
                        "name": "kitchenservice",
                        "aggregates": [
                            {
                                "id": "agg-kitchen",
                                "name": "Kitchen",
                                "policies": [],
                                "commands": [
                                    {
                                        "id": "cmd-prepare-kitchen",
                                        "name": "PrepareKitchen",
                                        "api_verb": "POST",
                                        "outputEvents": [
                                            {
                                                "id": "evt-kitchen-prepared",
                                                "name": "KitchenPrepared"
                                            }
                                        ]
                                    }
                                ],
                                "events": [
                                    {
                                        "id": "evt-kitchen-prepared",
                                        "name": "KitchenPrepared",
                                    }
                                ]
                            }
                        ]
                    },
                    {
                        "id": "bc-table",
                        "name": "tableservice",
                        "aggregates": [
                            {
                                "id": "agg-table",
                                "name": "Table",
                                "policies": [],
                                "commands": [
                                    {
                                        "id": "cmd-assign-table",
                                        "name": "AssignTable",
                                        "api_verb": "PATCH",
                                        "outputEvents": [
                                            {
                                                "id": "evt-table-assigned",
                                                "name": "TableAssigned"
                                            }
                                        ]
                                    }
                                ],
                                "events": [
                                    {
                                        "id": "evt-table-assigned",
                                        "name": "TableAssigned",
                                    }
                                ]
                            }
                        ]
                    }
                ]
            }),
            "description": description_with_line_numbers,
            "user_preferred_language": "English"
        }

    def _build_json_example_output_format(self) -> Dict[str, Any]:
        return {
            "extractedPolicies": [
                {
                    "name": "AutoTableAssignment",
                    "alias": "Automatic Table Assignment",
                    "reason": "Fulfills the requirement that when a customer creates a reservation, the system must automatically assign an appropriate table.",
                    "fromEventId": "evt-reservation-created",
                    "toEventIds": ["evt-table-assigned"],
                    "refs": [[["7", "automatically"], ["7", "table"]]]
                },
                {
                    "name": "KitchenPreparation",
                    "alias": "Kitchen Preparation Notification",
                    "reason": "Implements a Pub/Sub pattern to notify the kitchen for preparation once a reservation is confirmed. This is defined in the relationship between the Reservation and Kitchen contexts.",
                    "fromEventId": "evt-reservation-confirmed",
                    "toEventIds": ["evt-kitchen-prepared"],
                    "refs": [[["8", "Kitchen"], ["8", "for"]], [["30", "publishes"], ["30", "preparation"]]]
                }
            ]
        }
    
    def _build_json_user_query_input_format(self) -> Dict[str, Any]:
        inputs = self.client.get("inputs")
        return {
            "summarized_es_value": XmlUtil.from_dict(inputs.get("summarizedESValue")),
            "description": inputs.get("description"),
            "user_preferred_language": self.client.get("preferredLanguage")
        }