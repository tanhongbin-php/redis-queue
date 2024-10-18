<?php

namespace Thb\Redis;

interface MiddlewareInterface
{
    public function process(array $request, callable $next): array;
}
