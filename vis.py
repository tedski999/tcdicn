import asyncio
async def main(server):
        try:
                import pygame
                import sys
                pygame.init()
                width, height = 600, 600
                screen = pygame.display.set_mode((width, height))
                pygame.display.set_caption("tcdicn")
                circle_radius = 20
                circle_x = 0
                circle_y = height // 2
                clock = pygame.time.Clock()
                speed = 5
                while True:
                    print("game loop")
                    for event in pygame.event.get():
                        if event.type == pygame.QUIT:
                            pygame.quit()
                            sys.exit()
                    circle_x += speed
                    if circle_x > width:
                        circle_x = -circle_radius
                    screen.fill((255, 255, 255))
                    pygame.draw.circle(screen, (0, 0, 255), (int(circle_x), int(circle_y)), circle_radius)
                    pygame.display.flip()
                    await asyncio.sleep(1)
        except SystemExit as e:
                print("SystemExit received, exiting game loop")
