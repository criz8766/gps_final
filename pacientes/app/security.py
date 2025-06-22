import os
import httpx
from fastapi import HTTPException, Security
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt, JWTError
from dotenv import load_dotenv # Necesitas python-dotenv en requirements.txt

# Construir la ruta al archivo .env que está en la carpeta 'pacientes'
# __file__ es la ruta a security.py (pacientes/app/security.py)
# os.path.dirname(__file__) es pacientes/app/
# os.path.dirname(os.path.dirname(__file__)) es pacientes/
env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path=env_path)

AUTH0_DOMAIN = os.getenv("AUTH0_DOMAIN")
API_AUDIENCE = os.getenv("AUTH0_API_AUDIENCE")
ALGORITHMS = ["RS256"]

reusable_oauth2 = HTTPBearer()
jwks_cache = None

async def get_jwks():
    global jwks_cache
    if AUTH0_DOMAIN is None: # Verificación temprana
        print("Error: AUTH0_DOMAIN no está configurado.")
        raise HTTPException(status_code=500, detail="Configuración de Auth0 (dominio) incompleta en el servidor.")

    if jwks_cache is None:
        async with httpx.AsyncClient() as client:
            try:
                jwks_url = f"https://{AUTH0_DOMAIN}/.well-known/jwks.json"
                print(f"Intentando obtener JWKS desde: {jwks_url}") # Log para depuración
                response = await client.get(jwks_url)
                response.raise_for_status()
                jwks_cache = response.json()
                print("JWKS obtenidos y cacheados exitosamente.") # Log para depuración
            except httpx.HTTPStatusError as e:
                print(f"Error HTTP al obtener JWKS desde {e.request.url!r}: {e.response.status_code} - {e.response.text}")
                raise HTTPException(status_code=500, detail="Error interno al obtener claves de firma del token.")
            except Exception as e:
                print(f"Excepción no esperada al obtener JWKS: {e}")
                raise HTTPException(status_code=500, detail="Error interno del servidor (JWKS).")
    return jwks_cache

async def validate_token(token: HTTPAuthorizationCredentials = Security(reusable_oauth2)):
    if AUTH0_DOMAIN is None or API_AUDIENCE is None:
        print("Error: AUTH0_DOMAIN o API_AUDIENCE no están configurados.") # Log para depuración
        raise HTTPException(
            status_code=500, detail="Variables de configuración de Auth0 no encontradas en el servidor."
        )

    if token is None:
        raise HTTPException(status_code=401, detail="No se proporcionó token de autorización")

    token_value = token.credentials

    try:
        jwks = await get_jwks()
        if not jwks or "keys" not in jwks: # Verificación adicional
            print("Error: JWKS no se pudieron obtener o están en formato incorrecto.")
            raise HTTPException(status_code=500, detail="Error al obtener la configuración de claves de firma.")

        unverified_header = jwt.get_unverified_header(token_value)
        rsa_key = {}
        for key in jwks["keys"]:
            if key["kid"] == unverified_header["kid"]:
                rsa_key = {
                    "kty": key["kty"],
                    "kid": key["kid"],
                    "use": key["use"],
                    "n": key["n"],
                    "e": key["e"]
                }
                break

        if rsa_key:
            print("Clave RSA encontrada para la validación del token.") # Log para depuración
            payload = jwt.decode(
                token_value,
                rsa_key,
                algorithms=ALGORITHMS,
                audience=API_AUDIENCE,
                issuer=f"https://{AUTH0_DOMAIN}/"
            )
            print("Token validado exitosamente.") # Log para depuración
            return payload
        else:
            print("Error: No se pudo encontrar la clave de firma apropiada para el token.") # Log para depuración
            # Si el token es válido pero el 'kid' no coincide, es un problema.
            # Si el token viene de otro issuer, es un problema.
            all_kids_in_jwks = [key.get("kid") for key in jwks.get("keys", [])]
            print(f"Token 'kid': {unverified_header.get('kid')}. Available 'kids' in JWKS: {all_kids_in_jwks}")
            raise HTTPException(status_code=401, detail="Token inválido: No se pudo encontrar la clave de firma.")

    except jwt.ExpiredSignatureError:
        print("Error: Token ha expirado.")
        raise HTTPException(status_code=401, detail="Token ha expirado.")
    except jwt.JWTClaimsError as e:
        print(f"Error en claims JWT (audience/issuer): {e}")
        raise HTTPException(status_code=401, detail=f"Token inválido: Claims incorrectas ({str(e)}).")
    except JWTError as e:
        print(f"Error de JWT genérico: {e}")
        raise HTTPException(status_code=401, detail=f"Token inválido: {str(e)}")
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Excepción no esperada durante validación de token: {e}")
        # Podría ser un error de red al obtener JWKS si la caché está vacía y get_jwks falla de nuevo
        raise HTTPException(status_code=500, detail=f"Error interno del servidor al validar el token: {str(e)}")