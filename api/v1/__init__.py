from fastapi import APIRouter

from api.v1.chunk_retrieval.routes import router as rag_router
from api.v1.rephrase_retrieval.routes import router as rephrase_router


api_v1_router = APIRouter(prefix="/api/v1")

# Register v1 routers
api_v1_router.include_router(rag_router)
api_v1_router.include_router(rephrase_router)
