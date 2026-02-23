import { useSpring, animated } from "@react-spring/web";

/**
 * An SVG rect that smoothly animates y/height changes when data updates.
 * All other props are passed through directly.
 */
export function AnimatedBar({
  x,
  y,
  width,
  height,
  ...rest
}: React.SVGProps<SVGRectElement> & {
  x: number;
  y: number;
  width: number;
  height: number;
}) {
  const style = useSpring({
    to: { y, height },
    config: { tension: 210, friction: 20 },
  });

  return (
    <animated.rect
      x={x}
      y={style.y}
      width={width}
      height={style.height}
      {...rest}
    />
  );
}
