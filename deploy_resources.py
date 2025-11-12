"""Deploy agent brick resources (Genie/MAS/KA) from configuration."""

import json
import logging
import time
from pathlib import Path

from databricks.sdk import WorkspaceClient

from agent_bricks_service import AgentBricksManager, get_tile_example_queue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Deploy agent brick resources from bricks_conf.json."""
    # Read configuration from current working directory
    # Note: __file__ is not available in Databricks jobs, so we use cwd
    import os
    bricks_conf_path = Path(os.getcwd()) / 'bricks_conf.json'

    if not bricks_conf_path.exists():
        logger.info("No bricks_conf.json found, skipping agent brick deployment")
        return

    with open(bricks_conf_path, 'r') as f:
        bricks_conf = json.load(f)

    # Initialize Databricks client and manager
    w = WorkspaceClient()
    manager = AgentBricksManager(w)

    # Track newly created resource IDs for MAS agent mapping
    genie_space_id = None
    ka_tile_id = None
    ka_endpoint_name = None

    # Deploy Genie Space
    if 'genie_space' in bricks_conf:
        logger.info("Deploying Genie space...")
        genie_data = bricks_conf['genie_space']
        genie_conf = genie_data['config']
        genie_name = genie_conf['display_name']

        # Search by name to find if it already exists
        existing_genie = manager.genie_find_by_name(genie_name)

        if existing_genie:
            logger.info(f"Genie space '{genie_name}' already exists with space_id: {existing_genie.space_id}")
            genie_space_id = existing_genie.space_id
            # Skip adding questions/benchmarks/instructions for existing space
        else:
            # Create new Genie space
            try:
                result = manager.genie_create(
                    display_name=genie_name,
                    warehouse_id=genie_conf['warehouse_id'],
                    table_identifiers=genie_conf.get('table_identifiers', []),
                    description=genie_conf.get('description'),
                    run_as_type=genie_conf.get('run_as_type', 'VIEWER')
                )
                genie_space_id = result['space_id']
                logger.info(f"Genie space created: {genie_space_id}")

                # Add sample questions
                sample_questions = genie_data.get('sample_questions', [])
                if sample_questions:
                    question_texts = [q['question_text'] for q in sample_questions]
                    manager.genie_add_sample_questions_batch(genie_space_id, question_texts)
                    logger.info(f"Added {len(question_texts)} sample questions")

                # Add benchmarks
                benchmarks = genie_data.get('benchmarks', [])
                if benchmarks:
                    manager.genie_add_benchmarks_batch(genie_space_id, benchmarks)
                    logger.info(f"Added {len(benchmarks)} benchmarks")

                # Add instructions
                instructions = genie_data.get('instructions', [])
                for instr in instructions:
                    instr_type = instr['instruction_type']
                    if instr_type == 'TEXT_INSTRUCTION':
                        manager.genie_add_text_instruction(genie_space_id, instr['content'], instr['title'])
                    elif instr_type == 'SQL_INSTRUCTION':
                        manager.genie_add_sql_instruction(genie_space_id, instr['title'], instr['content'])
                    elif instr_type == 'CERTIFIED_ANSWER':
                        manager.genie_add_sql_function(genie_space_id, instr['content'])
                if instructions:
                    logger.info(f"Added {len(instructions)} instructions")
            except Exception as e:
                logger.error(f"Failed to create Genie space: {e}")
                raise

    # Deploy Knowledge Assistant
    if 'knowledge_assistant' in bricks_conf:
        logger.info("Deploying Knowledge Assistant...")
        ka_data = bricks_conf['knowledge_assistant']
        ka_conf = ka_data['config']['knowledge_assistant']
        ka_name = ka_conf['tile']['name']

        # Search by name to find if it already exists
        existing_ka = manager.find_by_name(ka_name)

        if existing_ka:
            logger.info(f"Knowledge Assistant '{ka_name}' already exists with tile_id: {existing_ka.tile_id}")
            ka_tile_id = existing_ka.tile_id
            # Get the endpoint name for MAS
            ka_details = manager.ka_get(ka_tile_id)
            ka_endpoint_name = ka_details.get('knowledge_assistant', {}).get('tile', {}).get('serving_endpoint_name')
            # Skip adding examples for existing KA
        else:
            # Create new Knowledge Assistant
            try:
                result = manager.ka_create(
                    name=ka_name,
                    knowledge_sources=ka_data.get('knowledge_sources', []),
                    description=ka_conf['tile'].get('description'),
                    instructions=ka_conf['tile'].get('instructions')
                )
                ka_tile_id = result['knowledge_assistant']['tile']['tile_id']
                ka_endpoint_name = result['knowledge_assistant']['tile'].get('serving_endpoint_name')
                logger.info(f"Knowledge Assistant created: {ka_tile_id}, endpoint: {ka_endpoint_name}")

                # Add examples - check if endpoint is ready first
                examples = ka_data.get('examples', [])
                if examples:
                    questions = [
                        {
                            'question': ex['question'],
                            'guideline': ex.get('guidelines', [None])[0] if ex.get('guidelines') else None
                        }
                        for ex in examples
                    ]

                    # Check endpoint status
                    endpoint_status = manager.ka_get_endpoint_status(ka_tile_id)
                    logger.info(f"KA endpoint status: {endpoint_status}")

                    if endpoint_status == 'ONLINE':
                        # Endpoint ready - add examples immediately
                        manager.ka_add_examples_batch(ka_tile_id, questions)
                        logger.info(f"Added {len(questions)} example questions")
                    else:
                        # Endpoint not ready - enqueue for background processing
                        logger.info(f"KA endpoint not ready (status: {endpoint_status}), enqueueing {len(questions)} questions for background processing")
                        queue = get_tile_example_queue()
                        queue.enqueue(ka_tile_id, manager, questions, tile_type='KA')
                        logger.info("Questions enqueued - will be added automatically when KA is ready")
            except Exception as e:
                logger.error(f"Failed to create Knowledge Assistant: {e}")
                raise

    # Deploy Multi-Agent Supervisor
    if 'multi_agent_supervisor' in bricks_conf:
        logger.info("Deploying Multi-Agent Supervisor...")
        mas_data = bricks_conf['multi_agent_supervisor']
        mas_conf = mas_data['config']['multi_agent_supervisor']
        mas_name = mas_conf['tile']['name']

        # Build agents array using the newly created/found resource IDs
        agents = []
        original_agents = mas_conf.get('agents', [])

        for agent in original_agents:
            # Update agent with newly created resource IDs
            if agent.get('agent_type') == 'serving-endpoint' and ka_endpoint_name:
                # This is the KA agent
                agents.append({
                    'name': agent.get('name', 'Knowledge Assistant'),
                    'description': agent.get('description'),
                    'agent_type': 'serving-endpoint',
                    'serving_endpoint': {'name': ka_endpoint_name}
                })
                logger.info(f"Mapped KA agent to endpoint: {ka_endpoint_name}")
            elif agent.get('agent_type') == 'genie-space' and genie_space_id:
                # This is the Genie agent
                agents.append({
                    'name': agent.get('name', 'Genie Data Explorer'),
                    'description': agent.get('description'),
                    'agent_type': 'genie-space',
                    'genie_space': {'id': genie_space_id}
                })
                logger.info(f"Mapped Genie agent to space_id: {genie_space_id}")

        if not agents:
            logger.warning("No agents configured for MAS - skipping MAS creation")
        else:
            # Search by name to find if MAS already exists
            existing_mas = manager.mas_find_by_name(mas_name)

            if existing_mas:
                logger.info(f"Multi-Agent Supervisor '{mas_name}' already exists with tile_id: {existing_mas.tile_id}")
                # Try to update it
                try:
                    manager.mas_update(
                        tile_id=existing_mas.tile_id,
                        name=mas_name,
                        description=mas_conf['tile'].get('description'),
                        instructions=mas_conf['tile'].get('instructions'),
                        agents=agents
                    )
                    logger.info(f"Multi-Agent Supervisor updated: {existing_mas.tile_id}")
                except Exception as e:
                    logger.warning(f"Failed to update MAS: {e}. The MAS configuration may not have changed.")
            else:
                # Create new MAS
                try:
                    result = manager.mas_create(
                        name=mas_name,
                        agents=agents,
                        description=mas_conf['tile'].get('description'),
                        instructions=mas_conf['tile'].get('instructions')
                    )
                    mas_tile_id = result['multi_agent_supervisor']['tile']['tile_id']
                    logger.info(f"Multi-Agent Supervisor created: {mas_tile_id}")

                    # Add examples - check if endpoint is ready first
                    examples = mas_data.get('examples', [])
                    if examples:
                        questions = [
                            {
                                'question': ex['question'],
                                'guideline': ex.get('guidelines', [None])[0] if ex.get('guidelines') else None
                            }
                            for ex in examples
                        ]

                        # Check endpoint status
                        endpoint_status = manager.mas_get_endpoint_status(mas_tile_id)
                        logger.info(f"MAS endpoint status: {endpoint_status}")

                        if endpoint_status == 'ONLINE':
                            # Endpoint ready - add examples immediately
                            manager.mas_add_examples_batch(mas_tile_id, questions)
                            logger.info(f"Added {len(questions)} example questions")
                        else:
                            # Endpoint not ready - enqueue for background processing
                            logger.info(f"MAS endpoint not ready (status: {endpoint_status}), enqueueing {len(questions)} questions for background processing")
                            queue = get_tile_example_queue()
                            queue.enqueue(mas_tile_id, manager, questions, tile_type='MAS')
                            logger.info("Questions enqueued - will be added automatically when MAS is ready")
                except Exception as e:
                    logger.error(f"Failed to create Multi-Agent Supervisor: {e}")
                    raise

    # Wait for the queue to finish processing all enqueued examples
    queue = get_tile_example_queue()
    if len(queue.queue) > 0:
        logger.info(f"Waiting for {len(queue.queue)} tiles to finish processing examples...")
        max_wait_time = 1800  # 30 minutes max
        start_time = time.time()
        check_interval = 30  # Check every 30 seconds

        while len(queue.queue) > 0:
            elapsed = time.time() - start_time
            if elapsed > max_wait_time:
                logger.warning(f"Timeout after {elapsed:.0f}s - {len(queue.queue)} tiles still processing")
                logger.warning("Background queue will continue processing. Examples will be added when endpoints are ready.")
                break

            logger.info(f"Still waiting for {len(queue.queue)} tiles to finish... ({elapsed:.0f}s elapsed)")
            time.sleep(check_interval)

        if len(queue.queue) == 0:
            logger.info("All examples processed successfully!")

    logger.info("Agent brick deployment completed successfully!")


if __name__ == '__main__':
    main()
